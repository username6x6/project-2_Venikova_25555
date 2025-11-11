from typing import Any, Dict, List, Optional, Tuple

from prettytable import PrettyTable

from src.decorators import create_cacher
from src.primitive_db.core import (
    _get_schema,
    create_table,
    drop_table,
    help_text,
    list_tables,
)
from src.primitive_db.core import delete as core_delete
from src.primitive_db.core import insert as core_insert
from src.primitive_db.core import select as core_select
from src.primitive_db.core import update as core_update
from src.primitive_db.parser import parse_command
from src.primitive_db.utils import (
    META_PATH,
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
)


def _print_table(schema: List[Dict[str, str]], rows: List[Dict[str, Any]]) -> None:
    headers = [c["name"] for c in schema]
    t = PrettyTable()
    t.field_names = headers
    for r in rows:
        t.add_row([r.get(h) for h in headers])
    print(t)


def run():
    metadata: Dict[str, Any] = load_metadata(META_PATH)
    # Кэшер для SELECT
    select_cache = create_cacher()

    print("База данных запущена. Введите команду. help для справки.")

    while True:
        try:
            user_input = input(">>> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not user_input:
            continue

        # Парсинг команд
        try:
            cmd = parse_command(user_input)
        except ValueError as ve:
            print(f"Ошибка: {ve}")
            continue

        # Выполнение команд
        try:
            ctype = cmd["cmd"]
            match ctype:
                case "help":
                    print(help_text())

                case "exit":
                    break

                case "list_tables":
                    tabs = list_tables(metadata)
                    print("tables - " + (", ".join(tabs) if tabs else ""))

                case "create_table":
                    table_name = cmd["table"]
                    columns = cmd["columns"]
                    metadata = create_table(metadata, table_name, columns)
                    save_metadata(META_PATH, metadata)

                case "drop_table":
                    table_name = cmd["table"]
                    metadata = drop_table(metadata, table_name)
                    save_metadata(META_PATH, metadata)
                    # Сброс кэша на всякий случай
                    select_cache = create_cacher()

                case "insert":
                    table = cmd["table"]
                    values = cmd["values"]  # уже приведены к Python типам парсером
                    rows = load_table_data(table)
                    rows = core_insert(metadata, table, rows, values)
                    save_table_data(table, rows)
                    # Сброс кэша после изменения данных
                    select_cache = create_cacher()
                    if rows:
                        new_id = rows[-1]["ID"]
                        print(f'Запись с ID={new_id} успешно добавлена '
                              'в таблицу "{table}".')

                case "select":
                    table = cmd["table"]
                    where = cmd.get("where")
                    rows = load_table_data(table)
                    schema = _get_schema(metadata, table)
                    # Ключ кэша: (table, where-как-кортеж)
                    where_key: Optional[Tuple[Tuple[str, Any], ...]] = None
                    if where:
                        where_key = tuple(sorted(where.items()))
                    key = (table, where_key)

                    def _value():
                        return core_select(rows, where)

                    result = select_cache(key, _value)
                    if result:
                        _print_table(schema, result)
                    else:
                        print("Нет данных по заданному запросу.")

                case "update":
                    table = cmd["table"]
                    set_clause = cmd["set"]
                    where = cmd["where"]
                    rows = load_table_data(table)
                    changed = core_update(metadata, table, rows, set_clause, where)
                    save_table_data(table, rows)
                    # Сброс кэша после изменения данных
                    select_cache = create_cacher()
                    if changed == 1 and "ID" in where:
                        print(f'Запись с ID={where["ID"]} в таблице "{table}" '
                              'успешно обновлена.')
                    else:
                        print(f"Обновлено записей: {changed}.")

                case "delete":
                    table = cmd["table"]
                    where = cmd["where"]
                    rows = load_table_data(table)
                    deleted = core_delete(rows, where)
                    save_table_data(table, rows)
                    # Сброс кэша после изменения данных
                    select_cache = create_cacher()
                    if deleted == 1 and "ID" in where:
                        print(f'Запись с ID={where["ID"]} успешно удалена '
                              'из таблицы "{table}".')
                    else:
                        print(f"Удалено записей: {deleted}.")

                case "info":
                    table = cmd["table"]
                    schema = _get_schema(metadata, table)
                    rows = load_table_data(table)
                    cols = ", ".join(f'{c["name"]}:{c["type"]}' for c in schema)
                    print(f"Таблица: {table}")
                    print(f"Столбцы: {cols}")
                    print(f"Количество записей: {len(rows)}")

                case _:
                    print("Неизвестная команда. help для справки.")

        except ValueError as ve:
            # На случай ошибок парсинга/валидации вне ядра
            print(f"Ошибка: {ve}")

    print("Выход из программы.")
