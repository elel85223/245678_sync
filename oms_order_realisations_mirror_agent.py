"""
Агент зеркала: реализации заказов интернет-магазина из ЛКД -> litics.pf_oms_order_realisations.

Источник (база ПрофиСферы, только чтение):
  prod_oms.public.orders           - номер и дата реализации (realisation_number, realisation_date),
                                     номер заказа ИМ, дистрибьютор;
  prod_sellers.public.distributors - distributors.id -> bd_litics_id (= litics.partners.id).

С миграции prod_oms 2026_10_05_192839_denormillize_realisations_to_orders (деплой 15.10.2026)
таблицы order_realisations нет: у заказа одна реализация, ее номер и дата лежат в orders.
Ключ зеркала id = orders.id (= order_id). Своего времени создания и изменения у реализации нет,
created_at и updated_at в зеркале пишутся NULL.

Поиск документа Sell Out (litics.sales) у дистрибьютора за дату реализации, без учета регистра:
  1 - номер = код документа без суффикса даты (_ГГГГММДД или _ДД-ММ-ГГГГ);
  2 - код без суффикса даты заканчивается на "_номер" или "-номер";
  3 - совпали только цифры (без ведущих нулей).
Правила проверяются по порядку. Если по первому сработавшему правилу подходит больше одного
документа, документ не определяется. Результат - sale_code (litics.sales.saleCode) и match_rule;
Power BI соединяет строки Sell Out с зеркалом по partner_id + sale_code + дата.

Запись: только новые и изменившиеся строки, одной транзакцией. Строки, которых нет в источнике,
удаляются: зеркало должно точно повторять реализации ЛКД.

Логи публичного репозитория видит любой пользователь GitHub, поэтому агент печатает только
счетчики, id реализаций и имена колонок, без номеров документов, заказов и названий.

Переменные окружения:
  LKD_PG_HOST, LKD_PG_USER, LKD_PG_PASSWORD             обязательные
  LKD_PG_PORT                                           по умолчанию 6432
  LKD_PG_SSLMODE                                        по умолчанию require
  LITICS_DB_HOST, LITICS_DB_USER, LITICS_DB_PASSWORD    обязательные
  LITICS_DB_PORT                                        по умолчанию 3306
  LITICS_DB_NAME                                        по умолчанию litics
  MODE                                                  dry (по умолчанию) или write
"""
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg
import pymysql

# ================= НАСТРОЙКИ =================
TARGET_TABLE = "pf_oms_order_realisations"
KEYS = ["id"]
COLS = [
    "id", "order_id", "order_number", "lkd_distributor_id", "partner_id",
    "realisation_number", "realisation_date", "sale_code", "match_rule",
    "created_at", "updated_at",
]
DELETE_MISSING = True  # зеркало для Power BI: удаленные в ЛКД реализации удаляются

REALISATIONS_SQL = """
    SELECT
        o.id,
        o.id                                                  AS order_id,
        o.number::text                                        AS order_number,
        COALESCE(o.assigned_distributor_id, o.distributor_id) AS lkd_distributor_id,
        o.realisation_number,
        o.realisation_date
    FROM public.orders o
    WHERE o.realisation_number IS NOT NULL
      AND btrim(o.realisation_number) <> ''
      AND o.realisation_date IS NOT NULL
"""
DISTRIBUTORS_SQL = "SELECT d.id, d.bd_litics_id FROM public.distributors d"
# =============================================

MSK = timezone(timedelta(hours=3))
CHUNK = 1000        # строк в одном пакете записи
SHOW_EXAMPLES = 20  # сколько id печатать в списках
DATE_SUFFIX = re.compile(r"_([0-9]{8}|[0-9]{2}[-.][0-9]{2}[-.][0-9]{4})$")
NON_DIGITS = re.compile(r"[^0-9]")
RULE_NAMES = {
    1: "1 номер = код без суффикса даты",
    2: "2 префикс + номер",
    3: "3 совпали только цифры",
}


def env(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and not value:
        sys.exit(f"Не задана переменная окружения {name}")
    return value


def pg_connect(dbname):
    conn = psycopg.connect(
        host=env("LKD_PG_HOST", required=True),
        port=int(env("LKD_PG_PORT", "6432")),
        dbname=dbname,
        user=env("LKD_PG_USER", required=True),
        password=env("LKD_PG_PASSWORD", required=True),
        sslmode=env("LKD_PG_SSLMODE", "require"),
        # Несуществующий путь: libpq не подхватит чужой root.crt и не станет проверять сертификат
        sslrootcert=env("LKD_PG_SSLROOTCERT", "lkd-no-root-cert"),
        connect_timeout=15,
    )
    conn.prepare_threshold = None  # без серверных prepared statements (пулер соединений)
    return conn


def litics_connect():
    return pymysql.connect(
        host=env("LITICS_DB_HOST", required=True),
        port=int(env("LITICS_DB_PORT", "3306")),
        user=env("LITICS_DB_USER", required=True),
        password=env("LITICS_DB_PASSWORD", required=True),
        database=env("LITICS_DB_NAME", "litics"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        connect_timeout=15,
    )


def q(name):
    return "`" + name.replace("`", "``") + "`"


def msk(value):
    """timestamp без зоны (UTC) -> МСК без долей секунды."""
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(MSK).replace(tzinfo=None, microsecond=0)
    return (value + timedelta(hours=3)).replace(microsecond=0)


def to_partner_id(value):
    """prod_sellers.distributors.bd_litics_id (varchar) -> litics.partners.id (int)."""
    if value is None:
        return None
    text = str(value).strip()
    return int(text) if text.isdigit() else None


def digits(text):
    return NON_DIGITS.sub("", text).lstrip("0")


def match_document(number, codes):
    """Код документа Sell Out для номера реализации.

    Возвращает (код, правило). (None, 0) - по первому сработавшему правилу подходит
    больше одного документа; (None, None) - документ не найден.
    """
    num = number.strip().casefold()
    if not num:
        return None, None
    num_digits = digits(num)
    found = {1: set(), 2: set(), 3: set()}
    for code in codes:
        base = DATE_SUFFIX.sub("", code.strip()).casefold()
        if base == num:
            found[1].add(code)
        elif len(base) > len(num) and base.endswith(num) and base[-len(num) - 1] in "_-":
            found[2].add(code)
        elif num_digits and digits(base) == num_digits:
            found[3].add(code)
    for rule in (1, 2, 3):
        if len(found[rule]) == 1:
            return next(iter(found[rule])), rule
        if len(found[rule]) > 1:
            return None, 0
    return None, None


def norm(value):
    """Приведение для сравнения источника и зеркала."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, float):
        return Decimal(repr(value))
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    return value


def fmt_ids(keys):
    return ", ".join(str(k[0] if isinstance(k, tuple) else k) for k in keys[:SHOW_EXAMPLES])


def load_source():
    with pg_connect("prod_sellers") as conn:
        partner_by_distributor = {
            row[0]: to_partner_id(row[1]) for row in conn.execute(DISTRIBUTORS_SQL).fetchall()
        }
    with pg_connect("prod_oms") as conn:
        cur = conn.execute(REALISATIONS_SQL)
        names = [d.name for d in cur.description]
        raw = [dict(zip(names, r)) for r in cur.fetchall()]

    rows = {}
    for r in raw:
        key = (r["id"],)
        if key in rows:
            sys.exit(f"В источнике повторяется id реализации {r['id']}")
        rows[key] = {
            "id": r["id"],
            "order_id": r["order_id"],
            "order_number": r["order_number"],
            "lkd_distributor_id": r["lkd_distributor_id"],
            "partner_id": partner_by_distributor.get(r["lkd_distributor_id"]),
            "realisation_number": (r["realisation_number"] or "").strip(),
            "realisation_date": r["realisation_date"],
            "sale_code": None,
            "match_rule": None,
            "created_at": None,  # у реализации в orders нет своего времени создания
            "updated_at": None,  # и изменения
        }
    return rows


def load_sellout(lit, db, rows):
    """Коды документов Sell Out по (partnerId, дата) и последняя дата Sell Out по partnerId."""
    pairs = {(r["partner_id"], r["realisation_date"]) for r in rows.values() if r["partner_id"] is not None}
    if not pairs:
        return {}, {}
    partners = sorted({p for p, _d in pairs})
    date_from = min(d for _p, d in pairs)
    date_to = max(d for _p, d in pairs) + timedelta(days=1)
    in_list = ", ".join(["%s"] * len(partners))
    sales = f"{q(db)}.{q('sales')}"

    codes = {}
    with lit.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT s.partnerId AS partner_id, s.saleCode AS sale_code, DATE(s.saleDate) AS sale_day "
            f"FROM {sales} s "
            f"WHERE s.partnerId IN ({in_list}) AND s.saleDate >= %s AND s.saleDate < %s",
            partners + [date_from, date_to],
        )
        for r in cur.fetchall():
            key = (r["partner_id"], r["sale_day"])
            if key in pairs and r["sale_code"]:
                codes.setdefault(key, []).append(r["sale_code"])

        cur.execute(
            "SELECT s.partnerId AS partner_id, MAX(DATE(s.saleDate)) AS last_day "
            f"FROM {sales} s "
            f"WHERE s.partnerId IN ({in_list}) AND s.saleDate >= %s "
            "GROUP BY s.partnerId",
            partners + [date_from],
        )
        last_day = {r["partner_id"]: r["last_day"] for r in cur.fetchall()}
    return codes, last_day


def apply_matching(rows, codes, last_day):
    stats = {1: [], 2: [], 3: [], "ambiguous": [], "not_found": [], "no_data": [], "no_partner": []}
    for key in sorted(rows):
        row = rows[key]
        if row["partner_id"] is None:
            stats["no_partner"].append(row["id"])
            continue
        code, rule = match_document(row["realisation_number"],
                                    codes.get((row["partner_id"], row["realisation_date"]), []))
        if code is not None:
            row["sale_code"], row["match_rule"] = code, rule
            stats[rule].append(row["id"])
        elif rule == 0:
            stats["ambiguous"].append(row["id"])
        else:
            last = last_day.get(row["partner_id"])
            if last is None or last < row["realisation_date"]:
                stats["no_data"].append(row["id"])
            else:
                stats["not_found"].append(row["id"])
    return stats


def print_matching(stats, total):
    found = len(stats[1]) + len(stats[2]) + len(stats[3])
    print(f"\nПоиск документа Sell Out: реализаций {total}, найдено {found}")
    for rule in (1, 2, 3):
        print(f"  правило {RULE_NAMES[rule]}: {len(stats[rule])}")
    labels = [
        ("ambiguous", "подходит больше одного документа"),
        ("not_found", "не найден, Sell Out за дату есть"),
        ("no_data", "Sell Out за дату еще не пришел"),
        ("no_partner", "у дистрибьютора нет bd_litics_id"),
    ]
    for name, label in labels:
        ids = stats[name]
        line = f"  {label}: {len(ids)}"
        if ids:
            line += f" (id реализаций: {fmt_ids(ids)})"
        print(line)


def probe_rights(lit, db):
    """Пробные запросы: что учетке Litics можно делать с sales и таблицей-зеркалом.

    Тестовая строка с id = -1 удаляется в той же транзакции, транзакция откатывается.
    """
    table = f"{q(db)}.{q(TARGET_TABLE)}"
    checks = [
        ("sales SELECT", f"SELECT 1 FROM {q(db)}.{q('sales')} LIMIT 1"),
        (f"{TARGET_TABLE} SELECT", f"SELECT 1 FROM {table} LIMIT 1"),
        (f"{TARGET_TABLE} INSERT",
         f"INSERT INTO {table} (id, order_id, order_number, realisation_number, realisation_date, synced_at) "
         "VALUES (-1, -1, '', '', '2000-01-01', NOW())"),
        (f"{TARGET_TABLE} UPDATE", f"UPDATE {table} SET synced_at = synced_at WHERE id = -1"),
        (f"{TARGET_TABLE} DELETE", f"DELETE FROM {table} WHERE id = -1"),
    ]
    result = []
    with lit.cursor() as cur:
        for name, sql in checks:
            try:
                cur.execute(sql)
                cur.fetchall()
                result.append(f"  {name}: есть")
            except pymysql.err.MySQLError as exc:
                code = exc.args[0] if exc.args else "?"
                result.append(f"  {name}: нет (ошибка MySQL {code})")
    lit.rollback()
    return result


def load_mirror(lit, db):
    with lit.cursor() as cur:
        cur.execute("SELECT " + ", ".join(q(c) for c in COLS) + f" FROM {q(db)}.{q(TARGET_TABLE)}")
        return {tuple(r[k] for k in KEYS): r for r in cur.fetchall()}


def diff(new, old):
    return [c for c in COLS if norm(new[c]) != norm(old.get(c))]


def write(lit, db, upserts, deletes):
    table = f"{q(db)}.{q(TARGET_TABLE)}"
    col_sql = ", ".join(q(c) for c in COLS)
    placeholders = ", ".join(["%s"] * len(COLS))
    updates = [f"{q(c)} = VALUES({q(c)})" for c in COLS if c not in KEYS]
    updates.append("`synced_at` = VALUES(`synced_at`)")
    upsert_sql = (
        f"INSERT INTO {table} ({col_sql}, `synced_at`) "
        f"VALUES ({placeholders}, NOW()) ON DUPLICATE KEY UPDATE " + ", ".join(updates)
    )
    delete_sql = f"DELETE FROM {table} WHERE " + " AND ".join(f"{q(k)} = %s" for k in KEYS)
    params = [[row[c] for c in COLS] for row in upserts]
    try:
        with lit.cursor() as cur:
            for i in range(0, len(params), CHUNK):
                cur.executemany(upsert_sql, params[i:i + CHUNK])
            for i in range(0, len(deletes), CHUNK):
                cur.executemany(delete_sql, [list(k) for k in deletes[i:i + CHUNK]])
        lit.commit()
    except Exception:
        lit.rollback()
        raise


def main():
    mode = env("MODE", "dry").strip().lower()
    if mode not in ("dry", "write"):
        sys.exit(f"MODE должен быть dry или write, получено: {mode}")
    db = env("LITICS_DB_NAME", "litics")
    print(f"Режим: {mode}, запуск {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"Источник: prod_oms (orders.realisation_number, realisation_date), prod_sellers (distributors); "
          f"цель: {TARGET_TABLE}, ключ: {', '.join(KEYS)}")

    source = load_source()
    lit = litics_connect()
    try:
        if mode == "dry":
            print("Права учетки Litics:")
            print("\n".join(probe_rights(lit, db)))

        codes, last_day = load_sellout(lit, db, source)
        stats = apply_matching(source, codes, last_day)
        print_matching(stats, len(source))

        mirror = load_mirror(lit, db)
        new_keys, changed = [], []
        for key in sorted(source):
            if key not in mirror:
                new_keys.append(key)
            else:
                d = diff(source[key], mirror[key])
                if d:
                    changed.append((key, d))
        missing = sorted(k for k in mirror if k not in source)

        print(f"\nСтрок в источнике: {len(source)}, в зеркале: {len(mirror)}")
        print(f"Новые: {len(new_keys)}, изменились: {len(changed)}, "
              f"без изменений: {len(source) - len(new_keys) - len(changed)}, "
              f"нет в источнике: {len(missing)}")
        if new_keys:
            print(f"Новые, id (первые {SHOW_EXAMPLES}): {fmt_ids(new_keys)}")
        if changed:
            print(f"Изменились, id: колонки (первые {SHOW_EXAMPLES}):")
            for key, d in changed[:SHOW_EXAMPLES]:
                print(f"  {key[0]}: {', '.join(d)}")
        if missing:
            action = "будут удалены" if DELETE_MISSING else "не удаляются"
            print(f"Есть в зеркале, нет в источнике ({action}), id: {fmt_ids(missing)}")

        upserts = [source[k] for k in new_keys] + [source[k] for k, _d in changed]
        deletes = missing if DELETE_MISSING else []
        if mode == "write" and (upserts or deletes):
            write(lit, db, upserts, deletes)
            print(f"\nЗаписано строк: {len(upserts)}, удалено: {len(deletes)}")
        elif mode == "write":
            print("\nЗаписывать нечего")
        else:
            print(f"\nРежим dry: запись не выполнялась, к записи {len(upserts)}, к удалению {len(deletes)}")
    finally:
        lit.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # GitHub Actions пометит прогон как упавший
        print(f"ОШИБКА: {type(exc).__name__}: {exc}")
        sys.exit(1)
