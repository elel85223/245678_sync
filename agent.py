"""
Агент обратной синхронизации статусов программы лояльности.

Логика:
  1. HTTP GET на эндпоинт ЛК /local/tools/gifts_status_export/
     с action=sync_and_export. Эндпоинт сам запускает gifts_sync.php
     (Google -> HL) и возвращает JSON со всеми записями HL-45.
  2. Дедуплицируем items по external_id (берём с max id).
  3. Открываем Google-таблицу через service account.
  4. Сопоставляем строки таблицы с записями HL по ключу
     R (УПД) + L (артикул) + F (ИНН).
  5. Обновляем столбец T одним батч-запросом:
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


def dedupe_items(items):
    """Дедупликация по external_id, берём запись с max id."""
    by_key = {}
    for item in items:
        ext = item.get("external_id", "")
        if not ext:
            continue
        if ext not in by_key or item["id"] > by_key[ext]["id"]:
            by_key[ext] = item

    if len(by_key) != len(items):
        log.warning(
            "Дедупликация: %d -> %d (удалено: %d)",
            len(items), len(by_key), len(items) - len(by_key),
        )

    return list(by_key.values())


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
    """Читает таблицу и строит индекс ключ -> (row_index, current_status)."""
    all_values = ws.get_all_values()
    log.info("Загружено строк из таблицы: %d", len(all_values))

    index = {}
    duplicates = []

    for i, row in enumerate(all_values, start=1):
        if i == 1:
            continue  # заголовок

        # Дополняем строку до нужной длины
        if len(row) < COL_STATUS:
            row = row + [""] * (COL_STATUS - len(row))

        inn = (row[COL_INN - 1] or "").strip()
        article = (row[COL_ARTICLE - 1] or "").strip()
        upd = (row[COL_UPD - 1] or "").strip()
        current_status = (row[COL_STATUS - 1] or "").strip()

        if not (inn and article and upd):
            continue  # неполная строка, пропускаем

        key = upd + "__" + article + "__" + inn
        if key in index:
            duplicates.append((i, key))
            continue
        index[key] = (i, current_status)

    if duplicates:
        log.warning("В Google-таблице найдено дублей ключа: %d", len(duplicates))
        for row_idx, key in duplicates[:5]:
            log.warning("  row %d: %s", row_idx, key)

    log.info("Уникальных строк с заполненным ключом: %d", len(index))
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


def compute_updates(items, sheet_index):
    """Сопоставляет HL-items со строками таблицы, готовит batch-updates."""
    updates = []
    not_found = []
    no_change = 0

    for item in items:
        key = item.get("external_id", "")
        if not key:
            continue

        if key not in sheet_index:
            not_found.append((key, item.get("id")))
            continue

        row_idx, current_status = sheet_index[key]
        new_status = format_status_text(item)
        if new_status is None:
            continue

        if current_status == new_status:
            no_change += 1
            continue

        updates.append({
            "range": "T" + str(row_idx),
            "values": [[new_status]],
        })

    log.info("Подготовлено обновлений: %d", len(updates))
    log.info("Уже актуальны (без изменений): %d", no_change)
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

    items = dedupe_items(items)

    ws = open_sheet()
    sheet_index = build_index(ws)
    updates = compute_updates(items, sheet_index)
    apply_updates(ws, updates)

    log.info("=== Готово ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("Агент завершился с ошибкой: %s", e)
        sys.exit(1)
