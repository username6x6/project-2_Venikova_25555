import re
from typing import Any, Dict, List, Optional


def _unquote(s: str) -> str:
    s = s.strip()
    if (len(s) >= 2) and ((s[0] == s[-1]) and s[0] in ("'", '"')):
        return s[1:-1]
    return s


def parse_scalar(token: str) -> Any:
    t = token.strip()
    # bool
    low = t.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    # int
    if re.fullmatch(r"[+-]?\d+", t):
        return int(t)
    # string (требуем кавычки для строк, как в задании)
    if (len(t) >= 2) and (t[0] in ("'", '"') and t[-1] == t[0]):
        return _unquote(t)
    # иначе — ошибка
    raise ValueError(f"Некорректное значение: {token} (строки должны быть в кавычках)")


def _split_csv_outside_quotes(s: str) -> List[str]:
    parts = []
    cur = []
    q = None
    for ch in s:
        if q:
            if ch == q:
                q = None
            cur.append(ch)
        else:
            if ch in ("'", '"'):
                q = ch
                cur.append(ch)
            elif ch == ",":
                parts.append("".join(cur).strip())
                cur = []
            else:
                cur.append(ch)
    if cur:
        parts.append("".join(cur).strip())
    return parts


def parse_values_list(values_segment: str) -> List[Any]:
    # ожидаем "( ... )"
    s = values_segment.strip()
    if not (s.startswith("(") and s.endswith(")")):
        raise ValueError("Ожидался список значений в скобках: (v1, v2, ...)")
    inner = s[1:-1]
    raw_vals = _split_csv_outside_quotes(inner)
    return [parse_scalar(rv) for rv in raw_vals]


def parse_where(where_segment: Optional[str]) -> Optional[Dict[str, Any]]:
    if not where_segment:
        return None
    # поддерживаем один предикат: <col> = <value>
    m = re.fullmatch(r"\s*(\w+)\s*=\s*(.+?)\s*\s*", where_segment)
    if not m:
        raise ValueError("Ожидалось условие вида: <колонка> = <значение>")
    col = m.group(1)
    val = parse_scalar(m.group(2))
    return {col: val}


def parse_set(set_segment: str) -> Dict[str, Any]:
    # поддерживаем несколько через запятую: a=1, b="x"
    assigns = _split_csv_outside_quotes(set_segment)
    res: Dict[str, Any] = {}
    for a in assigns:
        m = re.fullmatch(r"\s*(\w+)\s*=\s*(.+?)\s*", a)
        if not m:
            raise ValueError(f"Некорректное присваивание в set: {a}")
        col = m.group(1)
        val = parse_scalar(m.group(2))
        res[col] = val
    return res


# Командные парсеры
def parse_command(line: str) -> Dict[str, Any]:
    s = line.strip()
    low = s.lower()

    # insert into <table> values (...)
    m = re.fullmatch(r"\s*insert\s+into\s+(\w+)\s+values\s*(\(.+\))\s*", 
                     s, flags=re.IGNORECASE)
    if m:
        return {"cmd": "insert", "table": m.group(1), 
                "values": parse_values_list(m.group(2))}

    # select from <table> [where ...]
    m = re.fullmatch(r"\s*select\s+from\s+(\w+)\s*(?:where\s+(.+))?\s*", 
                     s, flags=re.IGNORECASE)
    if m:
        table = m.group(1)
        where = parse_where(m.group(2)) if m.group(2) else None
        return {"cmd": "select", "table": table, "where": where}

    # update <table> set ... where ...
    m = re.fullmatch(r"\s*update\s+(\w+)\s+set\s+(.+?)\s+where\s+(.+)\s*", 
                     s, flags=re.IGNORECASE)
    if m:
        return {"cmd": "update", "table": m.group(1), "set": parse_set(m.group(2)), 
                "where": parse_where(m.group(3))}

    # delete from <table> where ...
    m = re.fullmatch(r"\s*delete\s+from\s+(\w+)\s+where\s+(.+)\s*", 
                     s, flags=re.IGNORECASE)
    if m:
        return {"cmd": "delete", "table": m.group(1), "where": parse_where(m.group(2))}

    # info <table>
    m = re.fullmatch(r"\s*info\s+(\w+)\s*", s, flags=re.IGNORECASE)
    if m:
        return {"cmd": "info", "table": m.group(1)}

    # list_tables
    if re.fullmatch(r"\s*list_tables\s*", s, flags=re.IGNORECASE):
        return {"cmd": "list_tables"}

    # create_table <name> <col:type> ...
    if low.startswith("create_table "):
        parts = s.split()
        if len(parts) < 3:
            raise ValueError("Некорректная команда. Ожидались имя таблицы и столбцы.")
        return {"cmd": "create_table", "table": parts[1], "columns": parts[2:]}

    # drop_table <name>
    if low.startswith("drop_table "):
        parts = s.split()
        if len(parts) != 2:
            raise ValueError("Некорректная команда. Ожидалось имя таблицы.")
        return {"cmd": "drop_table", "table": parts[1]}

    # help / exit
    if re.fullmatch(r"\s*help\s*", s, flags=re.IGNORECASE):
        return {"cmd": "help"}
    if re.fullmatch(r"\s*exit\s*", s, flags=re.IGNORECASE):
        return {"cmd": "exit"}

    raise ValueError("Некорректная функция или формат команды")
