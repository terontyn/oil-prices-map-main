import re
import os
import datetime
import pytz
import pandas as pd
from src.config import Config

def get_msk_now():
    return datetime.datetime.now(pytz.timezone('Europe/Moscow'))

def get_msk_time_str():
    return get_msk_now().strftime("%d.%m.%Y %H:%M:%S")

def normalize_code(code):
    if code is None: return ""
    return str(code).upper().strip().replace('"', "").replace("\n", "")

def clean_price(value):
    try:
        s = str(value).strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
        return float(s) if float(s) > 0 else None
    except: return None

def parse_coordinates(coord_str):
    try: return float(str(coord_str).replace(",", ".").strip())
    except: return None

def get_fuel_category(fuel_name):
    if pd.isna(fuel_name) or not fuel_name: return None
    s = str(fuel_name).lower()
    # Проверка по приоритету из Config.CAT_CHECK_ORDER
    for cat in Config.CAT_CHECK_ORDER:
        if any(kw in s for kw in Config.FUEL_TYPES[cat]):
            return cat
    return None

def get_file_date_short(filepath):
    if not filepath or not os.path.exists(filepath): return "?"
    basename = os.path.basename(filepath)
    match = re.search(r'(\d{4})(\d{2})(\d{2})', basename)
    if match: return f"{match.group(3)}.{match.group(2)}"
    match = re.search(r'(\d{2})[._-](\d{2})', basename)
    if match: return f"{match.group(1)}.{match.group(2)}"
    try:
        ts = os.path.getmtime(filepath)
        return datetime.datetime.fromtimestamp(ts, pytz.timezone('Europe/Moscow')).strftime("%d.%m")
    except: return ""

def save_data_to_csv(markers, filename="report.csv"):
    if not markers: return None
    rows = []
    for m in markers:
        for f in m.get("fuels", []):
            rows.append({
                "Название": m.get("name"),
                "Компания": m.get("company", "").upper(),
                "Сегмент": m.get("company_segment", ""),
                "Топливо": f.get("name"),
                "Категория": f.get("cat"),
                "Цена": f.get("price"),
                "Дата": f.get("date")
            })
    df = pd.DataFrame(rows)
    out_path = os.path.join(Config.OUTPUT_DIR, filename)
    df.to_csv(out_path, index=False, sep=";", encoding="utf-8-sig")
    return out_path
