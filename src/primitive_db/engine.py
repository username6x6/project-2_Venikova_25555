from typing import Any, Dict, List

from prettytable import PrettyTable

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
    print("База данных запущена. Введите команду. help для справки.")

    while True:
        try:
            user_input = input(">>> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not user_input:
            continue

        try:
            cmd = parse_command(user_input)
        except ValueError as ve:
            print(f"Ошибка: {ve}")
            continue

        try:
            if cmd["cmd"] == "help":
                print(help_text())

            elif cmd["cmd"] == "exit":
                break

            elif cmd["cmd"] == "list_tables":
                tabs = list_tables(metadata)
                print("tables - " + (", ".join(tabs) if tabs else ""))

            elif cmd["cmd"] == "create_table":
                table_name = cmd["table"]
                columns = cmd["columns"]
                try:
                    metadata = create_table(metadata, table_name, columns)
                    save_metadata(META_PATH, metadata)
                except ValueError as ve:
                    print(f"Ошибка: {ve}")

            elif cmd["cmd"] == "drop_table":
                table_name = cmd["table"]
                metadata = drop_table(metadata, table_name)
                save_metadata(META_PATH, metadata)

            elif cmd["cmd"] == "insert":
                table = cmd["table"]
                values = cmd["values"]  # уже приведены к Python типам парсером
                rows = load_table_data(table)
                try:
                    rows = core_insert(metadata, table, rows, values)
                    save_table_data(table, rows)
                    new_id = rows[-1]["ID"]
                    print(f'Запись с ID={new_id} успешно добавлена '
                          'в таблицу \"{table}\".')
                except ValueError as ve:
                    print(f"Ошибка: {ve}")

            elif cmd["cmd"] == "select":
                table = cmd["table"]
                where = cmd.get("where")
                rows = load_table_data(table)
                # Схема нужна для порядка столбцов
                schema = _get_schema(metadata, table)
                result = core_select(rows, where)
                if result:
                    _print_table(schema, result)
                else:
                    print("Нет данных по заданному запросу.")

            elif cmd["cmd"] == "update":
                table = cmd["table"]
                set_clause = cmd["set"]
                where = cmd["where"]
                rows = load_table_data(table)
                try:
                    changed = core_update(metadata, table, rows, set_clause, where)
                    save_table_data(table, rows)
                    if changed == 1 and "ID" in where:
                        print(f'Запись с ID={where["ID"]} в таблице \"{table}\" '
                              'успешно обновлена.')
                    else:
                        print(f"Обновлено записей: {changed}.")
                except ValueError as ve:
                    print(f"Ошибка: {ve}")

            elif cmd["cmd"] == "delete":
                table = cmd["table"]
                where = cmd["where"]
                rows = load_table_data(table)
                deleted = core_delete(rows, where)
                save_table_data(table, rows)
                if deleted == 1 and "ID" in where:
                    print(f'Запись с ID={where["ID"]} успешно удалена '
                          'из таблицы \"{table}\".')
                else:
                    print(f"Удалено записей: {deleted}.")

            elif cmd["cmd"] == "info":
                table = cmd["table"]
                schema = _get_schema(metadata, table)
                rows = load_table_data(table)
                cols = ", ".join([f'{c["name"]}:{c["type"]}' for c in schema])
                print(f"Таблица: {table}")
                print(f"Столбцы: {cols}")
                print(f"Количество записей: {len(rows)}")

            else:
                print("Неизвестная команда. help для справки.")

        except ValueError as ve:
            print(f"Ошибка: {ve}")

    print("Выход из программы.")
