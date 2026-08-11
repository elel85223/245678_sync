# -*- coding: utf-8 -*-
"""
Агент синхронизации дистрибьюторов: Битрикс24 CRM -> litics.partners
"""

import os
import sys
import time
import requests
import pymysql

B24_WEBHOOK = os.environ["B24_WEBHOOK"].rstrip("/") + "/"
DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ.get("DB_NAME", "litics")
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

COUNTRY_RU_ENUM = "182039"          # UF_CRM_1668428521 = Россия
COMPANY_TYPES = {
    "OTHER": "Дистрибьютор",
    "11": "Дистрибьютор (не активный)",
}
F_COUNTRY = "UF_CRM_1668428521"
F_RM = "UF_CRM_1641999321"          # ID пользователя Б24 (РМ)
F_INN = "UF_CRM_1646855368887"
F_FED_DISTRICT = "UF_CRM_1650959251"  # ID элемента списка (инфоблок 119)
F_DIRECTION = "UF_CRM_1785492097"     # Направление / Организация (множественное)
FED_DISTRICT_IBLOCK = 119

DIRECTION_NAMES = {
    "5674": "ПРОТЕКО",
    "5675": "ПРОТЕКОЛАБ",
}
DIRECTION_PROTECO = "5674"

# Поля, участвующие в UPDATE (name и taxNumber намеренно исключены)
SYNC_FIELDS = [
    "company_type", "federal_district", "direction",
    "regional_manager_id", "regional_manager_name",
    "regional_manager_email", "regional_manager_phone",
]


def b24_call(method, params=None):
    """Вызов REST-метода Б24 с 3 повторами."""
    url = B24_WEBHOOK + method + ".json"
    for attempt in range(3):
        try:
            r = requests.post(url, json=params or {}, timeout=60)
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                raise RuntimeError(f"{method}: {data.get('error')} {data.get('error_description')}")
            return data
        except (requests.RequestException, ValueError) as e:
            if attempt == 2:
                raise
            print(f"  Повтор {method} после ошибки: {e}")
            time.sleep(3)


def direction_ids(company):
    """Значения поля Направление как множество строк."""
    raw = company.get(F_DIRECTION)
    if not raw:
        return set()
    if not isinstance(raw, list):
        raw = [raw]
    return {str(v) for v in raw if v}


def fetch_companies():
    """Компании РФ (Дистрибьютор / неактивный) с Направлением ПРОТЕКО."""
    companies = []
    skipped = 0
    for ctype in COMPANY_TYPES:
        start = 0
        while True:
            data = b24_call("crm.company.list", {
                "filter": {"COMPANY_TYPE": ctype, F_COUNTRY: COUNTRY_RU_ENUM,
                           "IS_MY_COMPANY": "N"},
                "select": ["ID", "TITLE", "COMPANY_TYPE",
                           F_RM, F_INN, F_FED_DISTRICT, F_DIRECTION],
                "start": start,
            })
            for c in data.get("result", []):
                if DIRECTION_PROTECO in direction_ids(c):
                    companies.append(c)
                else:
                    skipped += 1
                    print(f"  Пропуск id={c['ID']} ({c.get('TITLE')}): "
                          f"Направление={sorted(direction_ids(c)) or 'пусто'}")
            if "next" not in data:
                break
            start = data["next"]
            time.sleep(0.5)
    print(f"   Пропущено по Направлению: {skipped}")
    return companies


def fetch_fed_districts():
    """Справочник ФО: ID элемента списка -> название."""
    mapping = {}
    start = 0
    while True:
        data = b24_call("lists.element.get", {
            "IBLOCK_TYPE_ID": "lists",
            "IBLOCK_ID": FED_DISTRICT_IBLOCK,
            "start": start,
        })
        for el in data.get("result", []):
            mapping[str(el["ID"])] = el["NAME"]
        if "next" not in data:
            break
        start = data["next"]
        time.sleep(0.5)
    return mapping


def fetch_users(user_ids):
    """Данные РМ по списку ID: ФИО, email, рабочий телефон."""
    users = {}
    for uid in sorted(user_ids):
        data = b24_call("user.get", {"ID": uid})
        result = data.get("result", [])
        if not result:
            print(f"  Внимание: пользователь Б24 ID={uid} не найден")
            continue
        u = result[0]
        fio = " ".join(x for x in [u.get("LAST_NAME"), u.get("NAME"), u.get("SECOND_NAME")] if x)
        phone = u.get("WORK_PHONE") or u.get("PERSONAL_MOBILE") or None
        users[str(uid)] = {
            "name": fio or None,
            "email": u.get("EMAIL") or None,
            "phone": phone,
        }
        time.sleep(0.3)
    return users


def norm(v):
    """Пустые строки -> None, обрезка пробелов."""
    if v is None:
        return None
    v = str(v).strip()
    return v or None


def build_target_rows(companies, users, districts):
    """Целевое состояние: externalId -> поля partners."""
    rows = {}
    for c in companies:
        ext_id = str(c["ID"])
        rm_id = norm(c.get(F_RM))
        rm = users.get(rm_id, {}) if rm_id else {}
        fd_id = norm(c.get(F_FED_DISTRICT))
        dirs = sorted(direction_ids(c))
        rows[ext_id] = {
            "name": norm(c.get("TITLE")),
            "taxNumber": norm(c.get(F_INN)),
            "company_type": COMPANY_TYPES[c["COMPANY_TYPE"]],
            "federal_district": districts.get(fd_id) if fd_id else None,
            "direction": ", ".join(DIRECTION_NAMES.get(d, d) for d in dirs) or None,
            "regional_manager_id": int(rm_id) if rm_id and rm_id.isdigit() else None,
            "regional_manager_name": rm.get("name"),
            "regional_manager_email": rm.get("email"),
            "regional_manager_phone": rm.get("phone"),
        }
    return rows


def sync_db(target):
    conn = pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
        charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor, autocommit=False,
    )
    inserted = updated = unchanged = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, externalId, company_type, federal_district, direction, "
                "regional_manager_id, regional_manager_name, regional_manager_email, "
                "regional_manager_phone FROM partners WHERE externalId IN ({})".format(
                    ",".join(["%s"] * len(target))
                ),
                list(target.keys()),
            )
            existing = {r["externalId"]: r for r in cur.fetchall()}

            for ext_id, row in target.items():
                if ext_id in existing:
                    db_row = existing[ext_id]
                    changes = {}
                    for f in SYNC_FIELDS:
                        new_v = row[f]
                        old_v = db_row[f]
                        if f == "regional_manager_id":
                            old_v = int(old_v) if old_v is not None else None
                        else:
                            old_v = norm(old_v)
                        if new_v != old_v:
                            changes[f] = new_v
                    if changes:
                        set_sql = ", ".join(f"`{f}` = %s" for f in changes)
                        cur.execute(
                            f"UPDATE partners SET {set_sql} WHERE id = %s",
                            list(changes.values()) + [db_row["id"]],
                        )
                        updated += 1
                        print(f"UPDATE externalId={ext_id}: {', '.join(changes.keys())}")
                    else:
                        unchanged += 1
                else:
                    if row["taxNumber"] is None:
                        print(f"ПРОПУСК INSERT externalId={ext_id} ({row['name']}): нет ИНН в Б24")
                        continue
                    cur.execute(
                        "INSERT INTO partners (name, taxNumber, externalId, country_code, "
                        "company_type, federal_district, direction, regional_manager_id, "
                        "regional_manager_name, regional_manager_email, regional_manager_phone) "
                        "VALUES (%s, %s, %s, 'RU', %s, %s, %s, %s, %s, %s, %s)",
                        [row["name"], row["taxNumber"], ext_id, row["company_type"],
                         row["federal_district"], row["direction"],
                         row["regional_manager_id"], row["regional_manager_name"],
                         row["regional_manager_email"], row["regional_manager_phone"]],
                    )
                    inserted += 1
                    print(f"INSERT externalId={ext_id}: {row['name']}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return inserted, updated, unchanged


def main():
    print("1. Компании из Б24...")
    companies = fetch_companies()
    print(f"   Отобрано (Направление = ПРОТЕКО): {len(companies)}")
    if not companies:
        print("Компании не получены, синхронизация прервана.")
        sys.exit(1)

    print("2. Справочник федеральных округов...")
    districts = fetch_fed_districts()
    print(f"   Элементов: {len(districts)}")

    rm_ids = {norm(c.get(F_RM)) for c in companies if norm(c.get(F_RM))}
    print(f"3. Данные РМ ({len(rm_ids)} пользователей)...")
    users = fetch_users(rm_ids)

    target = build_target_rows(companies, users, districts)

    print("4. Синхронизация litics.partners...")
    inserted, updated, unchanged = sync_db(target)
    print(f"Итог: INSERT={inserted}, UPDATE={updated}, без изменений={unchanged}")


if __name__ == "__main__":
    main()
