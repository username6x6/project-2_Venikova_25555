import time
from functools import wraps
from typing import Any, Callable, Dict, Tuple


# Значения по умолчанию при ошибках для функций ядра
def _default_create_or_drop(metadata, *_, **__):
    return metadata

def _default_insert(_metadata, _table, rows, _values, **__):
    return rows

def _default_select(_rows, _where=None, **__):
    return []

def _default_update(*_, **__):
    return 0

def _default_delete(_rows, _where, **__):
    return 0

def _default_get_schema(*_, **__):
    return []

_DEFAULT_RETURNS: Dict[str, Callable[..., Any]] = {
    "create_table": _default_create_or_drop,
    "drop_table": _default_create_or_drop,
    "insert": _default_insert,
    "select": _default_select,
    "update": _default_update,
    "delete": _default_delete,
    "_get_schema": _default_get_schema,
    "list_tables": lambda *_a, **_k: [],
}


def handle_db_errors(func: Callable) -> Callable:
    """
    Ловит KeyError, ValueError, FileNotFoundError, печатает сообщение
    и возвращает безопасное значение по умолчанию.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (KeyError, ValueError, FileNotFoundError) as e:
            print(f"Ошибка: {e}")
            default = _DEFAULT_RETURNS.get(func.__name__)
            if default is not None:
                try:
                    return default(*args, **kwargs)
                except Exception:
                    return None
            return None
    return wrapper


def confirm_action(action_name: str) -> Callable:
    """
    Запрашивает подтверждение перед выполнением «опасного» действия.
    Если не 'y' — операция отменяется и возвращается значение по умолчанию.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            answer = input(f'Вы уверены, что хотите выполнить "{action_name}"? '
                           '[y/n]: ').strip().lower()
            if answer != "y":
                print("Операция отменена пользователем.")
                default = _DEFAULT_RETURNS.get(func.__name__)
                return default(*args, **kwargs) if default else None
            return func(*args, **kwargs)
        return wrapper
    return decorator

def log_time(func: Callable) -> Callable:
    """
    Замеряет и выводит время выполнения функции.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        try:
            return func(*args, **kwargs)
        finally:
            dt = time.monotonic() - start
            print(f"Функция {func.__name__} выполнилась за {dt:.3f} секунд.")
    return wrapper


def create_cacher() -> Callable[[Tuple[Any, ...], Callable[[], Any]], Any]:
    """
    Возвращает функцию cache_result(key, value_func), 
    кэширующую результаты value_func() по ключу.
    """
    cache: Dict[Tuple[Any, ...], Any] = {}
    def cache_result(key: Tuple[Any, ...], value_func: Callable[[], Any]) -> Any:
        if key in cache:
            return cache[key]
        value = value_func()
        cache[key] = value
        return value
    return cache_result
