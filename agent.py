"""
Агент обратной синхронизации статусов программы лояльности.

Логика:
  1. HTTP GET на эндпоинт ЛК /local/tools/gifts_status_export/
     с action=sync_and_export. Эндпоинт сам запускает gifts_sync.php
     (Google -> HL) и возвращает JSON со всеми записями HL-45.
  2. Группируем items по external_id, для каждой группы берём запись с max id
     (если по одному ключу в HL несколько записей — выбираем самую свежую).
  3. Открываем Google-таблицу через service account.
  4. Строим индекс ключ -> список строк таблицы с этим ключом
     (в Google законно может быть несколько одинаковых строк — например,
     одна клиника получила в одном УПД два экземпляра одного артикула двумя
     строками; обе строки реальные, обе должны получить статус).
  5. Обновляем столбец T одним батч-запросом для всех строк, у которых
     текущее значение отличается от целевого:
       SHIPPED   -> "Отправлен PROTECO"
       DELIVERED -> "Передан клинике DD.MM.YYYY"

Конфигурация через переменные окружения:
  LK_ENDPOINT_URL        базовый URL эндпоинта (без query)
  LK_GIFTS_STATUS_TOKEN  токен доступа к эндпоинту
  GOOGLE_SA_JSON         JSON service account целиком (одной строкой)
  GOOGLE_SHEET_ID        ID Google-таблицы
  GOOGLE_SHEET_NAME      название листа (по умолчанию "Лист1")
  HTTP_TIMEOUT           таймаут HTTP, сек (по умолчанию 600)
"""

import json
import logging
import os
import sys
from datetime import datetime

import gspread
import requests
from google.oauth2.service_account import Credentials


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("gifts-sync")


# Колонки Google-таблицы (1-индексированные)
COL_INN = 6       # F
COL_ARTICLE = 12  # L
COL_UPD = 18      # R
COL_STATUS = 20   # T


def env(name, default=None, required=True):
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        log.error("Не задана переменная окружения: %s", name)
        sys.exit(1)
    return value


def fetch_hl_items():
    """GET на эндпоинт ЛК. Возвращает список items со статусами."""
    base = env("LK_ENDPOINT_URL")
    token = env("LK_GIFTS_STATUS_TOKEN")
    timeout = int(env("HTTP_TIMEOUT", "600", required=False))

    url = base.rstrip("/") + "/"
    params = {"token": token, "action": "sync_and_export"}

    log.info("GET %s (action=sync_and_export)", base)
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    if not data.get("ok"):
        log.error("Эндпоинт вернул ok=false: %s", data.get("error"))
        sys.exit(1)

    sync_info = data.get("sync", {})
    log.info(
        "Синхронизация HL: executed=%s, duration=%s сек, exit_code=%s",
        sync_info.get("executed"),
        sync_info.get("duration"),
        sync_info.get("exit_code"),
    )

    counts = data.get("counts", {})
    log.info(
        "Записей в HL: total=%s, shipped=%s, delivered=%s",
        counts.get("total"),
        counts.get("shipped"),
        counts.get("delivered"),
    )

    return data.get("items", [])


def group_items_by_key(items):
    """
    Группировка items HL по external_id.
    Если по ключу несколько записей — берём с max id (самая свежая).
    Возвращает dict key -> item.
    """
    by_key = {}
    for item in items:
        ext = item.get("external_id", "")
        if not ext:
            continue
        if ext not in by_key or item["id"] > by_key[ext]["id"]:
            by_key[ext] = item

    collapsed = len(items) - len(by_key)
    if collapsed > 0:
        log.info(
            "В HL обнаружено записей с одинаковым ключом: %d (объединены, "
            "взята запись с max id)", collapsed,
        )
    return by_key


def open_sheet():
    """Авторизация и открытие листа."""
    sa_json = env("GOOGLE_SA_JSON")
    sheet_id = env("GOOGLE_SHEET_ID")
    sheet_name = env("GOOGLE_SHEET_NAME", "Лист1", required=False)

    try:
        creds_dict = json.loads(sa_json)
    except json.JSONDecodeError as e:
        log.error("GOOGLE_SA_JSON не является валидным JSON: %s", e)
        sys.exit(1)

    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet(sheet_name)
    log.info("Открыта таблица '%s', лист '%s'", sh.title, ws.title)
    return ws


def build_index(ws):
    """
    Читает таблицу и строит индекс ключ -> список (row_idx, current_status).
    В Google законно может быть несколько одинаковых строк — все попадают в список.
    """
    all_values = ws.get_all_values()
    log.info("Загружено строк из таблицы: %d", len(all_values))

    index = {}

    for i, row in enumerate(all_values, start=1):
        if i == 1:
            continue  # заголовок

        if len(row) < COL_STATUS:
            row = row + [""] * (COL_STATUS - len(row))

        inn = (row[COL_INN - 1] or "").strip()
        article = (row[COL_ARTICLE - 1] or "").strip()
        upd = (row[COL_UPD - 1] or "").strip()
        current_status = (row[COL_STATUS - 1] or "").strip()

        if not (inn and article and upd):
            continue  # неполная строка, пропускаем

        key = upd + "__" + article + "__" + inn
        index.setdefault(key, []).append((i, current_status))

    multi = {k: rows for k, rows in index.items() if len(rows) > 1}
    if multi:
        log.info(
            "В Google-таблице ключей с несколькими строками: %d "
            "(все строки будут обновлены)", len(multi),
        )
        for k, rows in list(multi.items())[:5]:
            row_idxs = ", ".join(str(r[0]) for r in rows)
            log.info("  ключ %s -> строки %s", k, row_idxs)

    total_rows = sum(len(rows) for rows in index.values())
    log.info(
        "Уникальных ключей: %d, всего строк с ключом: %d",
        len(index), total_rows,
    )
    return index


def format_status_text(item):
    """Текст для столбца T по статусу записи HL."""
    status = item.get("status")
    if status == "SHIPPED":
        return "Отправлен PROTECO"
    if status == "DELIVERED":
        delivered = item.get("delivered_date")
        if delivered:
            try:
                dt = datetime.strptime(delivered, "%Y-%m-%d")
                return "Передан клинике " + dt.strftime("%d.%m.%Y")
            except ValueError:
                log.warning("Не распарсил delivered_date: %s", delivered)
                return "Передан клинике " + delivered
        return "Передан клинике"
    return None


def compute_updates(items_by_key, sheet_index):
    """
    Сопоставляет items HL со строками таблицы.
    Для каждого ключа обновляет ВСЕ строки Google с этим ключом.
    """
    updates = []
    not_found = []
    no_change = 0
    rows_checked = 0

    for key, item in items_by_key.items():
        if key not in sheet_index:
            not_found.append((key, item.get("id")))
            continue

        new_status = format_status_text(item)
        if new_status is None:
            continue

        for row_idx, current_status in sheet_index[key]:
            rows_checked += 1
            if current_status == new_status:
                no_change += 1
                continue
            updates.append({
                "range": "T" + str(row_idx),
                "values": [[new_status]],
            })

    log.info("Строк к проверке: %d", rows_checked)
    log.info("Уже актуальны (без изменений): %d", no_change)
    log.info("Подготовлено обновлений: %d", len(updates))
    if not_found:
        log.warning("Не найдено в Google-таблице: %d", len(not_found))
        for key, hl_id in not_found[:10]:
            log.warning("  HL id=%s: %s", hl_id, key)

    return updates


def apply_updates(ws, updates):
    """Применяет batch-обновление."""
    if not updates:
        log.info("Нет изменений — таблица уже актуальна")
        return

    ws.batch_update(updates, value_input_option="USER_ENTERED")
    log.info("Применено обновлений: %d", len(updates))


def main():
    log.info("=== Запуск агента обратной синхронизации статусов ===")

    items = fetch_hl_items()
    if not items:
        log.warning("Эндпоинт не вернул ни одной записи — нечего синхронизировать")
        return

    items_by_key = group_items_by_key(items)

    ws = open_sheet()
    sheet_index = build_index(ws)
    updates = compute_updates(items_by_key, sheet_index)
    apply_updates(ws, updates)

    log.info("=== Готово ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("Агент завершился с ошибкой: %s", e)
        sys.exit(1)
