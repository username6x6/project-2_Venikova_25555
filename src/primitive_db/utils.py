import json
import os
from typing import Any, Dict, List

META_PATH = "db_meta.json"
DATA_DIR = "data"


def load_metadata(filepath: str = META_PATH) -> Dict[str, Any]:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(filepath: str = META_PATH, data: Dict[str, Any] = None) -> None:
    if data is None:
        data = {}
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def _table_path(table_name: str) -> str:
    os.makedirs(DATA_DIR, exist_ok=True)
    return os.path.join(DATA_DIR, f"{table_name}.json")


def load_table_data(table_name: str) -> List[Dict[str, Any]]:
    path = _table_path(table_name)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_table_data(table_name: str, data: List[Dict[str, Any]]) -> None:
    path = _table_path(table_name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
