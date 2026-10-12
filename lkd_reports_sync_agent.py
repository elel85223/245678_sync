"""
Агент зеркала отчетов нового ЛКД в Litics (prod_reports -> litics.lkd_reports).

Повторяет логику lkdReportsSyncRun старого ЛК (документ «Агент синхронизации зеркала
отчетов нового ЛКД»):
  1. PostgreSQL ЛКД: prod_sellers.distributors, prod_reports.reports + report_intervals.
  2. MySQL Litics: строки sales / rests под fileId отчета -> in_litics, litics_created_min.
  3. litics.lkd_reports: INSERT ... ON DUPLICATE KEY UPDATE только новых и изменившихся
     строк, сравнение по 16 полям; строки, которых нет в ЛКД, не удаляются.

Переменные окружения:
  LKD_PG_HOST, LKD_PG_USER, LKD_PG_PASSWORD             обязательные
  LKD_PG_PORT                                           по умолчанию 6432
  LKD_PG_SSLMODE                                        по умолчанию require
  LITICS_DB_HOST, LITICS_DB_USER, LITICS_DB_PASSWORD    обязательные
  LITICS_DB_PORT                                        по умолчанию 3306
  LITICS_DB_NAME                                        по умолчанию litics
  MODE                                                  dry (по умолчанию) - только сравнение,
                                                        write - сравнение и запись
"""
import os
import sys
from datetime import date, datetime, timedelta

import psycopg
import pymysql

MSK_SHIFT = timedelta(hours=3)
FILE_ID_OFFSET = 10_000_000
LOSS_STATUSES = (3, 6)  # 3 успешно, 6 скорректирован

# 16 полей сравнения, как в библиотеке старого ЛК
COMPARE_FIELDS = [
    "litics_file_id", "distributor_id", "partner_id", "kind", "deadline_report",
    "report_year", "report_month", "deadline_at", "status", "timeliness_status",
    "is_sent_to_bd_litics", "external_file_id", "submitted_at", "updated_at_lkd",
    "in_litics", "litics_created_min",
]
DATETIME_FIELDS = ("submitted_at", "updated_at_lkd", "litics_created_min")


# ---------- вспомогательные ----------

def env(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and not value:
        sys.exit(f"Не задана переменная окружения {name}")
    return value


def to_int(value):
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def to_msk(ts, shift=True):
    """timestamp ЛКД (UTC) -> МСК без долей секунды; shift=False - время уже в МСК."""
    if ts is None:
        return None
    ts = ts.replace(microsecond=0)
    return ts + MSK_SHIFT if shift else ts


def table_for(kind):
    return {2: "sales", 1: "rests"}.get(kind)


def norm(field, value):
    if value is None:
        return None
    if field in DATETIME_FIELDS:
        return value.replace(microsecond=0) if isinstance(value, datetime) else value
    if field == "deadline_at":
        return value.date() if isinstance(value, datetime) else value
    return int(value)


def diff(new, old):
    changes = []
    for field in COMPARE_FIELDS:
        a, b = norm(field, new[field]), norm(field, old.get(field))
        if a != b:
            changes.append((field, b, a))
    return changes


def fmt(value):
    if value is None:
        return "NULL"
    if isinstance(value, (datetime, date)):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    return str(value)


# ---------- подключения ----------

def pg_connect(dbname):
    conn = psycopg.connect(
        host=env("LKD_PG_HOST", required=True),
        port=int(env("LKD_PG_PORT", "6432")),
        dbname=dbname,
        user=env("LKD_PG_USER", required=True),
        password=env("LKD_PG_PASSWORD", required=True),
        sslmode=env("LKD_PG_SSLMODE", "require"),
        # require шифрует без проверки сертификата. Несуществующий путь не дает libpq
        # подхватить старый корневой сертификат Яндекса из %APPDATA%\postgresql\root.crt
        sslrootcert=env("LKD_PG_SSLROOTCERT", "lkd-no-root-cert"),
        connect_timeout=15,
    )
    conn.prepare_threshold = None  # пулер Odyssey: без серверных prepared statements
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


# ---------- чтение ЛКД ----------

def load_lkd():
    with pg_connect("prod_sellers") as conn:
        distributors = conn.execute(
            "SELECT id, bd_litics_id FROM public.distributors"
        ).fetchall()
    partner_by_distr = {int(d_id): to_int(bd_id) for d_id, bd_id in distributors}

    with pg_connect("prod_reports") as conn:
        reports = conn.execute(
            """
            SELECT r.id, r.distributor_id, r.type, r.deadline_report, r.status,
                   r.timeliness_status, r.is_sent_to_bd_litics, r.external_file_id,
                   r.created_at, r.updated_at,
                   i.year, i.month, i.deadline_at
            FROM public.reports r
            LEFT JOIN public.report_intervals i ON i.id = r.report_interval_id
            ORDER BY r.id
            """
        ).fetchall()
    return partner_by_distr, reports


def build_target(partner_by_distr, reports):
    target, skipped = {}, []
    for (rid, distr_id, rtype, deadline_report, status, timeliness, sent, ext_id,
         created_at, updated_at, year, month, deadline_at) in reports:
        rid = int(rid)
        report_year, report_month = to_int(year), to_int(month)
        if distr_id is None or created_at is None or report_year is None or report_month is None:
            skipped.append(rid)
            continue
        ext_id = to_int(ext_id)
        is_history = ext_id is not None  # история из старого ЛК: created_at уже в МСК
        target[rid] = {
            "litics_file_id": -(rid + FILE_ID_OFFSET),
            "distributor_id": int(distr_id),
            "partner_id": partner_by_distr.get(int(distr_id)),
            "kind": to_int(rtype),
            "deadline_report": to_int(deadline_report),
            "report_year": report_year,
            "report_month": report_month,
            "deadline_at": deadline_at,
            "status": to_int(status),
            "timeliness_status": to_int(timeliness),
            "is_sent_to_bd_litics": 1 if sent else 0,
            "external_file_id": ext_id,
            "submitted_at": to_msk(created_at, shift=not is_history),
            "updated_at_lkd": to_msk(updated_at),
            "in_litics": None,
            "litics_created_min": None,
        }
    return target, skipped


# ---------- Litics ----------

def fill_in_litics(lit, target):
    """in_litics и litics_created_min по строкам sales / rests (фильтр по partnerId - индекс)."""
    groups = {}
    for row in target.values():
        table = table_for(row["kind"])
        if row["partner_id"] is None or table is None:
            continue
        ids = groups.setdefault((table, row["partner_id"]), set())
        ids.add(row["litics_file_id"])
        if row["external_file_id"] is not None:
            ids.add(row["external_file_id"])

    found = {}
    with lit.cursor() as cur:
        for (table, partner_id), file_ids in sorted(groups.items()):
            ids = sorted(file_ids)
            placeholders = ",".join(["%s"] * len(ids))
            cur.execute(
                f"SELECT fileId, MIN(created_at) AS cmin FROM {table} "
                f"WHERE partnerId = %s AND fileId IN ({placeholders}) GROUP BY fileId",
                [partner_id, *ids],
            )
            for r in cur.fetchall():
                found[(table, int(r["fileId"]))] = r["cmin"]

    for row in target.values():
        table = table_for(row["kind"])
        if row["partner_id"] is None or table is None:
            continue  # без partner_id строки в Litics не ищутся, in_litics = NULL
        cmins = [
            found[(table, fid)]
            for fid in (row["litics_file_id"], row["external_file_id"])
            if fid is not None and (table, fid) in found
        ]
        row["in_litics"] = 1 if cmins else 0
        row["litics_created_min"] = min(cmins) if cmins else None


def load_mirror(lit):
    with lit.cursor() as cur:
        cur.execute("SELECT report_id, " + ", ".join(COMPARE_FIELDS) + " FROM lkd_reports")
        return {int(r["report_id"]): r for r in cur.fetchall()}


def write_rows(lit, rows):
    cols = ", ".join(COMPARE_FIELDS)
    placeholders = ", ".join(["%s"] * len(COMPARE_FIELDS))
    updates = ", ".join(f"{c} = VALUES({c})" for c in COMPARE_FIELDS)
    sql = (
        f"INSERT INTO lkd_reports (report_id, {cols}, source, synced_at) "
        f"VALUES (%s, {placeholders}, 'agent', NOW()) "
        f"ON DUPLICATE KEY UPDATE {updates}, source = VALUES(source), synced_at = VALUES(synced_at)"
    )
    params = [[rid] + [row[c] for c in COMPARE_FIELDS] for rid, row in rows]
    try:
        with lit.cursor() as cur:
            cur.executemany(sql, params)
        lit.commit()
    except Exception:
        lit.rollback()
        raise


# ---------- запуск ----------

def main():
    mode = env("MODE", "dry").strip().lower()
    if mode not in ("dry", "write"):
        sys.exit(f"MODE должен быть dry или write, получено: {mode}")
    print(f"Режим: {mode}, запуск {datetime.now():%Y-%m-%d %H:%M:%S}")

    partner_by_distr, reports = load_lkd()
    target, skipped = build_target(partner_by_distr, reports)

    lit = litics_connect()
    try:
        fill_in_litics(lit, target)
        mirror = load_mirror(lit)

        new_rows, changed_rows = [], []
        for rid in sorted(target):
            row = target[rid]
            if rid not in mirror:
                new_rows.append((rid, row))
            else:
                changes = diff(row, mirror[rid])
                if changes:
                    changed_rows.append((rid, row, changes))
        unchanged = len(target) - len(new_rows) - len(changed_rows)

        print(f"Отчетов в ЛКД: {len(reports)}, строк в зеркале: {len(mirror)}")
        print(f"Новые: {len(new_rows)}, изменились: {len(changed_rows)}, "
              f"без изменений: {unchanged}, пропущено: {len(skipped)}")

        if new_rows:
            print("\nНовые отчеты:")
            for rid, row in new_rows:
                print(f"  {rid}: partner {fmt(row['partner_id'])}, kind {row['kind']}, "
                      f"{row['report_year']}-{row['report_month']:02d}, "
                      f"deadline_report {row['deadline_report']}, status {row['status']}, "
                      f"подан {fmt(row['submitted_at'])}, in_litics {fmt(row['in_litics'])}")
        if changed_rows:
            print("\nИзменились:")
            for rid, _row, changes in changed_rows:
                parts = "; ".join(f"{f}: {fmt(old)} -> {fmt(new)}" for f, old, new in changes)
                print(f"  {rid}: {parts}")
        if skipped:
            print("\nПропущены (нет distributor_id, created_at или периода):",
                  ", ".join(map(str, skipped)))

        no_partner = sorted(rid for rid, row in target.items() if row["partner_id"] is None)
        if no_partner:
            print("\nБез partner_id (у дистрибьютора нет bd_litics_id):",
                  ", ".join(map(str, no_partner)))

        missing = sorted(set(mirror) - set(target))
        if missing:
            print("\nЕсть в зеркале, нет в ЛКД (не удаляются):", ", ".join(map(str, missing)))

        loss = sorted(rid for rid, row in target.items()
                      if row["status"] in LOSS_STATUSES and row["in_litics"] == 0)
        if loss:
            print("\nПоданы через ЛКД (статус 3 или 6), строк под fileId отчета в Литикс нет:",
                  ", ".join(map(str, loss)))

        to_write = new_rows + [(rid, row) for rid, row, _ in changed_rows]
        if mode == "write" and to_write:
            write_rows(lit, to_write)
            print(f"\nЗаписано строк: {len(to_write)}")
        elif mode == "write":
            print("\nЗаписывать нечего")
        else:
            print(f"\nРежим dry: запись не выполнялась, к записи {len(to_write)} строк")
    finally:
        lit.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # GitHub Actions пометит прогон как упавший
        print(f"ОШИБКА: {type(exc).__name__}: {exc}")
        sys.exit(1)
