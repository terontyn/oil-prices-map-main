# -*- coding: utf-8 -*-

import os
import glob
import json
import datetime
import re
import shutil
from pathlib import Path
from collections import defaultdict, Counter
from urllib.request import urlopen, Request
from urllib.error import URLError

import pdfplumber
import pandas as pd
from curl_cffi import requests as crequests
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────────────────────────────────────
# НАСТРОЙКИ
# ──────────────────────────────────────────────────────────────────────────────
DATA_DIR = os.getenv("DATA_DIR", "pdf_data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "public")
STATIONS_CSV = os.path.join(DATA_DIR, "stations.csv")
LUKOIL_STATIONS_CSV = os.path.join(DATA_DIR, "stations_lukoil.csv")
OTP_FILE = os.path.join(DATA_DIR, "stavkiOTP.txt")

# Параметры логистики
TARIFF_PER_KM = float(os.getenv("TARIFF_PER_KM", "170"))
TARIFF_PER_TON_KM = float(os.getenv("TARIFF_PER_TON_KM", "7"))
TRUCK_TONS = float(os.getenv("TRUCK_TONS", "25"))

# Параметры карты
MAP_PROVIDER = os.getenv("MAP_PROVIDER", "yandex_map")
MAP_CENTER_LAT = float(os.getenv("MAP_CENTER_LAT", "55.75"))
MAP_CENTER_LON = float(os.getenv("MAP_CENTER_LON", "37.62"))
MAP_ZOOM_START = int(os.getenv("MAP_ZOOM_START", "5"))

# URL
SPIMEX_URL = "https://spimex.com/markets/oil_products/trades/results/"
SPIMEX_BASE_URL = "https://spimex.com"
LUKOIL_PRICE_URL = "https://auto.lukoil.ru/ru/ForBusiness/wholesale/price"
LUKOIL_BASE_URL = "https://auto.lukoil.ru"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Топливо
FUEL_TYPES = {
    'Бензин': ['бензин', 'аи-92', 'аи-95', 'аи-98', 'аи-100', 'регуляр', 'премиум', 'euro', 'евро', 'аи'],
    'ДтА': ['дт-а', 'класс 4', 'вид 4', 'арктич', 'минус 44', 'минус 45', 'минус 50', 'минус 52', 'дта'],
    'ДтЗ': ['дт-з', 'класс 0', 'класс 1', 'класс 2', 'класс 3', 'зимн', 'минус 20', 'минус 26', 'минус 32', 'минус 35', 'минус 38', 'дтз'],
    'ДтЕ': ['дт-е', 'сорт e', 'сорт е', 'сорт f', 'минус 15'],
    'ДтЛ': ['дт-л', 'сорт c', 'сорт с', 'сорт d', 'летн', 'минус 5', 'минус 10', 'дтл'],
    'СУГ': ['суг', 'газ', 'пропан', 'бутан', 'lpg', 'сжиж']
}
_CAT_ORDER = ["Бензин", "ДтЛ", "ДтЕ", "ДтЗ", "ДтА", "СУГ"]

OTP_STATION_KEYS = [
    'лпдс володарская', 'нс солнечногорская', 'нс нагорная', 'лпдс сокур',
    'лпдс невская', 'лпдс черкассы', 'лпдс никольское-1', 'нп брянск',
    'лпдс воронеж', 'лпдс белгород'
]

# ──────────────────────────────────────────────────────────────────────────────
# УТИЛИТЫ
# ──────────────────────────────────────────────────────────────────────────────
def normalize_code(code):
    if code is None: return ""
    return str(code).upper().strip().replace('"', "").replace("\n", "")

def parse_coordinates(coord_str):
    try:
        return float(str(coord_str).replace(",", ".").strip())
    except:
        return None

def clean_price(value):
    try:
        s = str(value).strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
        v = float(s)
        return v if v > 0 else None
    except:
        return None

def get_fuel_category(fuel_name):
    if pd.isna(fuel_name) or not fuel_name: return None
    s = str(fuel_name).lower()
    for cat, keywords in FUEL_TYPES.items():
        if any(kw in s for kw in keywords): return cat
    return None

def get_file_date_short(filepath):
    basename = os.path.basename(filepath)
    match = re.search(r'(\d{4})(\d{2})(\d{2})', basename)
    if match: return f"{match.group(3)}.{match.group(2)}"
    match = re.search(r'(\d{2})[._-](\d{2})[._-](\d{4})', basename)
    if match: return f"{match.group(1)}.{match.group(2)}"
    try: return datetime.datetime.fromtimestamp(os.path.getmtime(filepath)).strftime("%d.%m")
    except: return ""

# ──────────────────────────────────────────────────────────────────────────────
# ЗАГРУЗКА ДАННЫХ
# ──────────────────────────────────────────────────────────────────────────────
def load_otp_prices():
    prices = {k: {"nalyv": 0, "storage": 0} for k in OTP_STATION_KEYS}
    if not os.path.exists(OTP_FILE): return prices
    try:
        with open(OTP_FILE, encoding="utf-8") as f:
            content = f.read().strip()
            if content.startswith('{'):
                data = json.loads(content)
                for k in OTP_STATION_KEYS:
                    if k in data:
                        prices[k] = {"nalyv": float(data[k].get("nalyv", 0) or 0), "storage": float(data[k].get("storage", 0) or 0)}
            else:
                for line in content.splitlines():
                    if '|' not in line: continue
                    parts = [p.strip() for p in line.split('|')]
                    if len(parts) >= 3:
                        key = parts[0].lower()
                        if key in prices:
                            prices[key] = {"nalyv": float(parts[1] or 0), "storage": float(parts[2] or 0)}
    except Exception as e:
        print(f"⚠️ Ошибка чтения OTP: {e}")
    return prices

def load_stations_reference(csv_path):
    if not os.path.exists(csv_path): return pd.DataFrame()
    df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str)
    col_code = next((c for c in df.columns if "code" in c.lower()), df.columns[0])
    col_lat = next((c for c in df.columns if "lat" in c.lower()), df.columns[1])
    col_lon = next((c for c in df.columns if "lon" in c.lower()), df.columns[2])
    col_name = next((c for c in df.columns if "name" in c.lower()), df.columns[3])
    
    stations = pd.DataFrame()
    stations["code"] = df[col_code].apply(normalize_code)
    stations["lat"] = df[col_lat].apply(parse_coordinates)
    stations["lon"] = df[col_lon].apply(parse_coordinates)
    stations["name"] = df[col_name].astype(str)
    stations["fuel_type"] = df.iloc[:, 4].astype(str) if len(df.columns) > 4 else ""
    return stations.dropna(subset=["lat", "lon"])

def load_lukoil_stations():
    if not os.path.exists(LUKOIL_STATIONS_CSV): return pd.DataFrame()
    try:
        df = pd.read_csv(LUKOIL_STATIONS_CSV, encoding="utf-8-sig", dtype=str)
    except:
        try: df = pd.read_csv(LUKOIL_STATIONS_CSV, encoding="cp1251", dtype=str)
        except: return pd.DataFrame()
        
    df.columns = [c.lower().strip() for c in df.columns]
    col_name = next((c for c in df.columns if "name" in c or "наименование" in c), None)
    col_lat = next((c for c in df.columns if "lat" in c or "широта" in c), None)
    col_lon = next((c for c in df.columns if "lon" in c or "долгота" in c), None)
    
    if not (col_name and col_lat and col_lon): return pd.DataFrame()

    out = pd.DataFrame()
    out["name"] = df[col_name].astype(str).str.strip()
    out["lat"] = df[col_lat].apply(parse_coordinates)
    out["lon"] = df[col_lon].apply(parse_coordinates)
    out["name_norm"] = out["name"].apply(lambda s: re.sub(r'\W+', '', s.lower()))
    return out.dropna(subset=["lat", "lon"])

# ──────────────────────────────────────────────────────────────────────────────
# ОБРАБОТКА ФАЙЛОВ
# ──────────────────────────────────────────────────────────────────────────────
def match_lukoil_station(name, stations_df):
    if stations_df.empty or not name: return None
    norm = re.sub(r'\W+', '', str(name).lower())
    match = stations_df[stations_df["name_norm"] == norm]
    if not match.empty: return match.iloc[0]
    
    stop_words = ['нефтебаза', 'ликард', 'ооо', 'зао', 'нб', 'гнс']
    def clean_core(s):
        s = s.lower()
        for w in stop_words: s = s.replace(w, '')
        return re.sub(r'\W+', '', s)
        
    core_name = clean_core(str(name))
    if len(core_name) < 3: return None
    
    for _, row in stations_df.iterrows():
        row_core = clean_core(row["name"])
        if core_name in row_core or row_core in core_name:
            return row
    return None

def download_latest_spimex():
    print("⏳ SPIMEX: Поиск бюллетеня...")
    try:
        r = crequests.get(SPIMEX_URL, impersonate="chrome116", timeout=20)
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            if 'oil_' in href.lower() and '.pdf' in href.lower():
                pdf_url = SPIMEX_BASE_URL + href if href.startswith('/') else href
                print(f"📥 Скачиваем: {pdf_url}")
                pdf_resp = crequests.get(pdf_url, impersonate="chrome116", timeout=30)
                out_path = os.path.join(DATA_DIR, "latest_spimex.pdf")
                with open(out_path, 'wb') as f:
                    f.write(pdf_resp.content)
                return out_path
    except Exception as e:
        print(f"❌ Ошибка SPIMEX: {e}")
    return None

def process_spimex_data(pdf_file, stations_df):
    if not pdf_file or stations_df.empty: return []
    date_str = get_file_date_short(pdf_file)
    results = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table: continue
            for row in table:
                if not row or len(row) < 5: continue
                code = normalize_code(row[0])
                price_best = clean_price(row[-3])
                price_mkt = clean_price(row[-4])
                if price_best is None: continue
                price = max(price_best, price_mkt) if price_mkt else price_best
                
                if code:
                    match = stations_df[stations_df["code"] == code]
                    if not match.empty:
                        s = match.iloc[0]
                        fuel_name = str(row[1]).strip() if len(row) > 1 else s.get("fuel_type", "")
                        cat = get_fuel_category(fuel_name)
                        if cat:
                            results.append({
                                "code": code, "name": s["name"], "lat": s["lat"], "lon": s["lon"],
                                "fuels": [{"name": fuel_name, "price": price, "cat": cat, "date": date_str}]
                            })
    return aggregate_markers_by_coordinates(results, company="spimex")

def fetch_lukoil_xlsx():
    try:
        headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "ru-RU"}
        req = Request(LUKOIL_PRICE_URL, headers=headers)
        html = urlopen(req, timeout=15).read().decode("utf-8", errors="replace")
        xlsx_paths = re.findall(r'href=["\']?(/FileSystem/[^"\'>\s]+\.xlsx[^"\'>\s]*)', html, re.I)
        for path in xlsx_paths:
            url = LUKOIL_BASE_URL + path
            if "dl=" not in url: url += "&dl=1" if "?" in url else "?dl=1"
            fname = os.path.basename(path).split('?')[0]
            if not fname.endswith('.xlsx'): fname += '.xlsx'
            
            if "spot" in path.lower() or "spot" in fname.lower(): prefix = "lukoil_spot_"
            elif "term" in path.lower() or "term" in fname.lower(): prefix = "lukoil_term_"
            else: prefix = "lukoil_"
                
            save_path = os.path.join(DATA_DIR, prefix + fname)
            if not os.path.exists(save_path):
                print(f"📥 Скачиваем Лукойл: {fname}")
                with open(save_path, "wb") as f:
                    f.write(urlopen(Request(url, headers=headers), timeout=30).read())
    except Exception as e:
        print(f"⚠️ Ошибка скачивания Лукойл: {e}")

    files = glob.glob(os.path.join(DATA_DIR, "*lukoil*.xlsx"))
    print(f"📂 Файлов Лукойл: {len(files)}")
    return files

def process_lukoil_xlsx(xlsx_file, stations_df):
    if not os.path.exists(xlsx_file): return []
    file_date = get_file_date_short(xlsx_file)
    results = []
    
    fname = os.path.basename(xlsx_file).lower()
    file_seg = "lukoil_spot" if "spot" in fname else "lukoil_term" if "term" in fname else "lukoil_other"
    print(f"Processing {os.path.basename(xlsx_file)} ({file_seg})...")

    try:
        df_raw = pd.read_excel(xlsx_file, header=None, engine="openpyxl")
        df_raw = df_raw.dropna(how="all").reset_index(drop=True)
        
        # ─── HYBRID PARSER: CHECK FOR WIDE HEADER ───
        wide_header_idx = -1
        fuel_map = {} # col_idx -> fuel_cat
        
        # Look for a row that contains >1 fuel keywords
        for r in range(min(15, len(df_raw))):
            row_vals = df_raw.iloc[r].astype(str).tolist()
            matches = {}
            for c_idx, val in enumerate(row_vals):
                cat = get_fuel_category(val)
                if cat: matches[c_idx] = cat
            
            if len(matches) >= 2:
                wide_header_idx = r
                fuel_map = matches
                break
        
        if wide_header_idx >= 0:
            print(f"  -> Detected WIDE format (Header at row {wide_header_idx})")
            # Re-detect station col
            col_st_idx = -1
            known_norms = set(stations_df["name_norm"])
            for c in df_raw.columns:
                if c in fuel_map: continue
                sample = df_raw[c].astype(str).head(30).tolist()
                match_count = sum(1 for x in sample if re.sub(r'\W+', '', x.lower()) in known_norms)
                if match_count > 0: col_st_idx = c; break
            
            if col_st_idx == -1: col_st_idx = 0
            
            # Start iterating from data rows
            last_valid_match = None
            for i in range(wide_header_idx + 1, len(df_raw)):
                row = df_raw.iloc[i]
                sname = str(row[col_st_idx]).strip()
                
                match = None
                if len(sname) > 3 and sname.lower() != 'nan':
                    match = match_lukoil_station(sname, stations_df)
                    if match is not None: last_valid_match = match
                
                if match is None and last_valid_match is not None:
                    match = last_valid_match
                    
                if match is not None:
                    # Iterate identified fuel columns
                    for c_idx, cat in fuel_map.items():
                        price = clean_price(row[c_idx])
                        if price:
                            results.append({
                                "code": f"LUK_{match['name_norm'][:10]}", "name": match["name"], 
                                "lat": match["lat"], "lon": match["lon"], 
                                "fuels": [{"name": cat, "price": price, "cat": cat, "date": file_date}],
                                "override_seg": file_seg
                            })
        else:
            print("  -> Detected LONG format (No header row found)")
            # ─── LONG FORMAT LOGIC ───
            col_st_idx = -1
            col_pr_idx = -1
            col_fl_idx = -1
            
            # Detect Columns
            known_norms = set(stations_df["name_norm"])
            best_match_count = 0
            for c in df_raw.columns:
                sample = df_raw[c].astype(str).head(50).tolist()
                cnt = sum(1 for x in sample if re.sub(r'\W+', '', x.lower()) in known_norms)
                if cnt > best_match_count: best_match_count = cnt; col_st_idx = c
            
            best_price_count = 0
            for c in df_raw.columns:
                if c == col_st_idx: continue
                sample = df_raw[c].head(50).tolist()
                cnt = sum(1 for x in sample if clean_price(x) and clean_price(x) > 10000)
                if cnt > best_price_count: best_price_count = cnt; col_pr_idx = c
            
            best_fuel_count = 0
            for c in df_raw.columns:
                if c == col_st_idx or c == col_pr_idx: continue
                sample = df_raw[c].astype(str).head(50).tolist()
                cnt = sum(1 for x in sample if get_fuel_category(x))
                if cnt > best_fuel_count: best_fuel_count = cnt; col_fl_idx = c
            
            if col_st_idx == -1: col_st_idx = 0
            if col_pr_idx == -1: col_pr_idx = len(df_raw.columns) - 1
            if col_fl_idx == -1: col_fl_idx = 1 if len(df_raw.columns) > 1 else -1
            
            last_valid_match = None
            for i, row in df_raw.iterrows():
                sname = str(row[col_st_idx]).strip()
                match = None
                if len(sname) > 3 and sname.lower() != 'nan':
                    match = match_lukoil_station(sname, stations_df)
                    if match is not None: last_valid_match = match
                
                if match is None and last_valid_match is not None:
                    match = last_valid_match
                
                if match is not None:
                    price = clean_price(row[col_pr_idx])
                    fname = str(row[col_fl_idx]) if col_fl_idx != -1 else ""
                    cat = get_fuel_category(fname)
                    
                    if not cat: # Fallback search row
                        for val in row:
                            c_try = get_fuel_category(str(val))
                            if c_try: cat = c_try; fname = str(val); break
                            
                    if price and cat:
                        results.append({
                            "code": f"LUK_{match['name_norm'][:10]}", "name": match["name"], 
                            "lat": match["lat"], "lon": match["lon"], 
                            "fuels": [{"name": fname, "price": price, "cat": cat, "date": file_date}],
                            "override_seg": file_seg
                        })

    except Exception as e:
        print(f"❌ Ошибка {xlsx_file}: {e}")
        
    print(f"   -> Найдено записей: {len(results)}")
    return aggregate_markers_by_coordinates(results, company="lukoil")

# ──────────────────────────────────────────────────────────────────────────────
# ГЕНЕРАЦИЯ HTML
# ──────────────────────────────────────────────────────────────────────────────
def aggregate_markers_by_coordinates(markers, precision=4, company=""):
    buckets = defaultdict(list)
    for m in markers:
        if m.get("lat") and m.get("lon"):
            key = (round(float(m["lat"]), precision), round(float(m["lon"]), precision))
            buckets[key].append(m)

    out = []
    for (lat, lon), items in buckets.items():
        uniq_names = sorted(list(set(x["name"] for x in items if x["name"])))
        name = " / ".join(uniq_names[:2]) + ("..." if len(uniq_names) > 2 else "")
        fuels, seen, cats, override_segs = [], set(), set(), set()
        
        for it in items:
            if "override_seg" in it: override_segs.add(it["override_seg"])
            for f in it.get("fuels", []):
                k = (f["name"], f["price"])
                if k not in seen:
                    seen.add(k); fuels.append(f); cats.add(f["cat"])
        
        fuels.sort(key=lambda x: (_CAT_ORDER.index(x["cat"]) if x["cat"] in _CAT_ORDER else 999, x["price"]))
        
        seg = "lukoil_other"
        if company == "spimex": seg = "spimex"
        elif company == "lukoil":
            if "lukoil_spot" in override_segs: seg = "lukoil_spot"
            elif "lukoil_term" in override_segs: seg = "lukoil_term"
            
        out.append({
            "code": f"PT_{lat}_{lon}".replace(".", "_"), "name": name,
            "lat": lat, "lon": lon,
            "fuels": fuels, "categories": list(cats),
            "company": company, "company_segment": seg
        })
    return out

def render_openlayers_html(markers, date_str, gen_time, otp_prices):
    markers_json = json.dumps(markers, ensure_ascii=False)
    otp_json = json.dumps(otp_prices, ensure_ascii=False)
    providers = {
        "yandex_map": {"title": "Yandex Карта", "url": "https://core-renderer-tiles.maps.yandex.net/tiles?l=map&v=23.09.14-0&x={x}&y={y}&z={z}&scale=1&lang=ru_RU", "visible": MAP_PROVIDER=="yandex_map"},
        "yandex_sat": {"title": "Yandex Спутник", "url": "https://core-sat-renderer-tiles.maps.yandex.net/tiles?l=sat&v=3.888.0&x={x}&y={y}&z={z}&lang=ru_RU", "visible": MAP_PROVIDER=="yandex_sat"},
        "osm": {"title": "OpenStreetMap", "url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png", "visible": MAP_PROVIDER=="openstreetmap"}
    }
    
    return f"""<!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>Карта нефтебаз AMN v1.3</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ol@v9.2.4/ol.css"/>
    <style>
        body, html, #map {{ margin: 0; width: 100%; height: 100%; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; overflow: hidden; }}
        .panel {{ position: fixed; top: 10px; left: 10px; z-index: 1000; background: rgba(255,255,255,0.95); width: 320px; max-height: 90vh; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.2); display: flex; flex-direction: column; }}
        .panel.closed {{ transform: translateX(-340px); }}
        .panel-header {{ background: #222; color: #fff; padding: 12px; border-radius: 12px 12px 0 0; display: flex; justify-content: space-between; align-items: center; cursor: pointer; }}
        .panel-body {{ overflow-y: auto; padding: 10px; flex: 1; }}
        .toggle-btn {{ position: absolute; left: 330px; top: 10px; background: #222; color: #fff; border: none; padding: 10px; border-radius: 50%; cursor: pointer; display: none; }}
        .panel.closed + .toggle-btn {{ display: block; }}
        .section {{ margin-bottom: 15px; border: 1px solid #eee; padding: 10px; border-radius: 8px; background: #fff; }}
        .section-title {{ font-weight: bold; font-size: 13px; margin-bottom: 8px; text-transform: uppercase; color: #555; }}
        input, select, button {{ width: 100%; padding: 8px; margin-top: 4px; box-sizing: border-box; border: 1px solid #ccc; border-radius: 6px; }}
        button {{ background: #222; color: #fff; border: none; cursor: pointer; font-weight: bold; }}
        button.secondary {{ background: #fff; color: #222; border: 1px solid #222; }}
        .checkbox-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 5px; font-size: 12px; }}
        .ol-popup {{ position: absolute; background-color: white; box-shadow: 0 1px 4px rgba(0,0,0,0.2); padding: 15px; border-radius: 10px; bottom: 45px; left: -50px; min-width: 280px; z-index: 5000; }}
        .ol-popup:after, .ol-popup:before {{ top: 100%; border: solid transparent; content: " "; height: 0; width: 0; position: absolute; pointer-events: none; }}
        .ol-popup:after {{ border-top-color: white; border-width: 10px; left: 48px; margin-left: -10px; }}
        .ol-popup:before {{ border-top-color: #cccccc; border-width: 11px; left: 48px; margin-left: -11px; }}
        .modal {{ display: none; position: fixed; z-index: 2000; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.5); align-items: center; justify-content: center; }}
        .modal-content {{ background-color: #fefefe; padding: 20px; border-radius: 10px; width: 500px; max-height: 80vh; overflow-y: auto; }}
        .otp-row {{ display: flex; justify-content: space-between; margin-bottom: 5px; border-bottom: 1px solid #eee; padding: 5px 0; }}
        .otp-input {{ width: 80px; text-align: right; }}
    </style>
</head>
<body>
<div class="panel" id="panel">
    <div class="panel-header" onclick="document.getElementById('panel').classList.toggle('closed')">
        <span>AMN v1.3</span><span>✕</span>
    </div>
    <div class="panel-body">
        <div style="font-size: 11px; color: #777; margin-bottom: 10px;">Дата: {date_str} <br> Обновлено: {gen_time}</div>
        <div class="section"><div class="section-title">Фильтры</div>
            <div class="checkbox-grid">
                <label><input type="checkbox" id="flt_spimex" checked onchange="applyFilters()"> SPIMEX</label>
                <label><input type="checkbox" id="flt_lukoil" checked onchange="applyFilters()"> Лукойл</label>
            </div>
            <hr>
            <div class="checkbox-grid">
                <label><input type="checkbox" id="flt_benz" checked onchange="applyFilters()"> Бензин</label>
                <label><input type="checkbox" id="flt_dtl" checked onchange="applyFilters()"> ДтЛ</label>
                <label><input type="checkbox" id="flt_dte" checked onchange="applyFilters()"> ДтЕ</label>
                <label><input type="checkbox" id="flt_dtz" checked onchange="applyFilters()"> ДтЗ</label>
                <label><input type="checkbox" id="flt_dta" checked onchange="applyFilters()"> ДтА</label>
                <label><input type="checkbox" id="flt_sug" checked onchange="applyFilters()"> СУГ</label>
            </div>
        </div>
        <div class="section"><div class="section-title">Логистика</div>
            <div style="display:flex;gap:5px"><input id="tariff" value="{TARIFF_PER_KM}"><input id="tonnage" value="{TRUCK_TONS}"></div>
            <div id="routeStatus" style="margin-top:8px;font-size:11px;background:#f9f9f9;padding:5px;">Выберите станцию</div>
            <div style="margin-top:5px"><button class="secondary" onclick="clearRoute()">Сброс</button></div>
        </div>
        <div class="section"><div class="section-title">Поиск лучшей цены</div>
            <div style="font-size:11px;margin-bottom:5px;">Alt+Click на карте</div>
            <div style="display:flex;gap:5px"><input id="finderRadius" value="300"><select id="finderFuel"><option value="ДтЛ">ДтЛ</option><option value="ДтЕ">ДтЕ</option><option value="ДтЗ">ДтЗ</option><option value="Бензин">Бензин</option></select></div>
            <div id="finderResult" style="margin-top:5px;font-size:11px"></div>
        </div>
        <div class="section"><div class="section-title">Настройки</div>
            <button class="secondary" onclick="document.getElementById('otpModal').style.display='flex'">Надбавки ОТП</button>
            <select id="mapProvider" onchange="changeMapLayer()" style="margin-top:5px"><option value="yandex_map">Yandex</option><option value="osm">OSM</option></select>
        </div>
        <input type="text" id="search" placeholder="Поиск..." onkeydown="if(event.key==='Enter') doSearch()">
    </div>
</div>
<button class="toggle-btn" onclick="document.getElementById('panel').classList.toggle('closed')">☰</button>
<div id="map"></div>
<div id="popup" class="ol-popup"><a href="#" id="popup-closer" style="position:absolute;top:2px;right:8px;text-decoration:none;color:#999;font-size:20px">✖</a><div id="popup-content"></div></div>
<div id="otpModal" class="modal"><div class="modal-content"><h3>Надбавки ОТП</h3><div id="otpList"></div><button onclick="saveOtp()" style="margin-top:15px">Сохранить</button><button class="secondary" onclick="document.getElementById('otpModal').style.display='none'">Закрыть</button></div></div>

<script src="https://cdn.jsdelivr.net/npm/ol@v9.2.4/dist/ol.js"></script>
<script>
const markers={markers_json}, otpPrices={otp_json}, providers={json.dumps(providers, ensure_ascii=False)};
let map, vectorSource, routeSource, activeStation=null, routePoints=[];

function getMarkerStyle(m) {{
    let color='#d32f2f', iconChar='💧';
    if(m.company_segment==='lukoil_spot') {{ color='#e11d48'; }}
    else if(m.company_segment==='lukoil_term') {{ color='#2563eb'; }}
    else if(m.company_segment==='spimex') {{ color='#0ea5e9'; iconChar='🏛'; }}
    
    const svg = `<svg width="32" height="42" viewBox="0 0 32 42" xmlns="http://www.w3.org/2000/svg"><filter id="s"><feDropShadow dx="0" dy="2" stdDeviation="2" flood-color="rgba(0,0,0,0.3)"/></filter><path d="M16 40 Q16 40 9 28 A 14 14 0 1 1 23 28 Q16 40 16 40 Z" fill="${{color}}" stroke="white" stroke-width="1.5" filter="url(#s)"/><circle cx="16" cy="14" r="8.5" fill="white"/><text x="16" y="19.5" font-size="13" text-anchor="middle" font-family="Arial">${{iconChar}}</text></svg>`;
    return new ol.style.Style({{ image: new ol.style.Icon({{ src: 'data:image/svg+xml;charset=utf-8,'+encodeURIComponent(svg), anchor:[0.5,1] }}) }});
}}

function initMap() {{
    const layers = Object.values(providers).map(p => new ol.layer.Tile({{ source: new ol.source.XYZ({{url:p.url}}), visible:p.visible, properties:{{name:p.title}} }}));
    vectorSource = new ol.source.Vector();
    routeSource = new ol.source.Vector();
    
    markers.forEach(m => {{
        const f = new ol.Feature({{ geometry: new ol.geom.Point(ol.proj.fromLonLat([m.lon, m.lat])), data:m }});
        f.setStyle(getMarkerStyle(m));
        vectorSource.addFeature(f);
    }});
    
    map = new ol.Map({{ target:'map', layers:[...layers, new ol.layer.Vector({{source:routeSource}}), new ol.layer.Vector({{source:vectorSource}})], view: new ol.View({{ center:ol.proj.fromLonLat([{MAP_CENTER_LON}, {MAP_CENTER_LAT}]), zoom:{MAP_ZOOM_START} }}) }});
    
    const overlay = new ol.Overlay({{ element:document.getElementById('popup'), autoPan:true }});
    map.addOverlay(overlay);
    document.getElementById('popup-closer').onclick = () => {{ overlay.setPosition(undefined); return false; }};
    
    map.on('singleclick', e => {{
        if(e.originalEvent.altKey) {{ handleFinder(e.coordinate); return; }}
        const f = map.forEachFeatureAtPixel(e.pixel, i=>i);
        if(f && f.get('data')) showPopup(f.get('data'), e.coordinate);
        else if(activeStation) addRoutePoint(e.coordinate);
        else overlay.setPosition(undefined);
    }});
    
    // OTP Init
    let h=''; for(let k in otpPrices) h+=`<div class="otp-row"><span>${{k}}</span><div><input class="otp-input" id="on_${{k}}" value="${{otpPrices[k].nalyv}}"><input class="otp-input" id="os_${{k}}" value="${{otpPrices[k].storage}}"></div></div>`;
    document.getElementById('otpList').innerHTML=h;
    applyFilters();
}}

function showPopup(d, c) {{
    const otp = getOtp(d.name);
    let rows = d.fuels.map(f => `<tr><td style="border-left:4px solid ${{getFuelColor(f.cat)}};padding-left:5px">${{f.name}}</td><td style="text-align:right;font-weight:bold">${{(f.price+otp).toLocaleString()}}₽</td></tr>`).join('');
    document.getElementById('popup-content').innerHTML = `<b>${{d.name}}</b><br><span style="color:#666">${{d.company}}</span><table style="width:100%;font-size:12px">${{rows}}</table><button style="margin-top:5px;width:100%" onclick="startRoute('${{d.code}}')">Логистика</button>`;
    map.getOverlayById('popup').setPosition(c);
}}

function getFuelColor(c) {{ return {{'Бензин':'red','ДтЛ':'blue','ДтЕ':'orange','ДтЗ':'lightblue','СУГ':'purple'}}[c]||'gray'; }}
function getOtp(n) {{ n=n.toLowerCase(); for(let k in otpPrices) if(n.includes(k)) return otpPrices[k].nalyv+otpPrices[k].storage; return 0; }}
function saveOtp() {{ for(let k in otpPrices) {{ otpPrices[k].nalyv=parseFloat(document.getElementById('on_'+k).value)||0; otpPrices[k].storage=parseFloat(document.getElementById('os_'+k).value)||0; }} document.getElementById('otpModal').style.display='none'; alert('Сохранено'); }}

function startRoute(code) {{ activeStation = vectorSource.getFeatures().find(f=>f.get('data').code===code).get('data'); routePoints=[]; routeSource.clear(); document.getElementById('routeStatus').innerHTML='Кликайте на карту...'; document.getElementById('popup-closer').click(); }}
function addRoutePoint(c) {{ routePoints.push(ol.proj.toLonLat(c)); routeSource.addFeature(new ol.Feature({{geometry:new ol.geom.Point(c)}})); if(window.event.shiftKey) calcRoute(); }}
function clearRoute() {{ routeSource.clear(); routePoints=[]; activeStation=null; document.getElementById('routeStatus').innerHTML='Выберите станцию'; }}

async function calcRoute() {{
    const coords = [[activeStation.lon, activeStation.lat], ...routePoints].map(p=>p.join(',')).join(';');
    const res = await fetch(`https://router.project-osrm.org/route/v1/driving/${{coords}}?overview=full&geometries=geojson`).then(r=>r.json());
    if(res.routes) {{
        const r = res.routes[0], km = r.distance/1000, cost = Math.round(km * document.getElementById('tariff').value);
        routeSource.addFeature((new ol.format.GeoJSON()).readFeature(r.geometry, {{dataProjection:'EPSG:4326', featureProjection:'EPSG:3857'}}));
        document.getElementById('routeStatus').innerHTML = `<b>${{km.toFixed(1)}} км</b> | <b>${{cost}} ₽</b>`;
    }}
}}

async function handleFinder(c) {{
    const center = ol.proj.toLonLat(c), rad = document.getElementById('finderRadius').value, type = document.getElementById('finderFuel').value;
    let cands = vectorSource.getFeatures().map(f=>f.get('data')).filter(d => {{
        const km = ol.sphere.getDistance(center, [d.lon, d.lat])/1000;
        const f = d.fuels.find(x => x.cat === type || (x.cat==='ДтЛ' && type==='ДтЛ'));
        return km < rad && f;
    }}).map(d => ({{d, f:d.fuels.find(x=>x.cat===type||(x.cat==='ДтЛ'&&type==='ДтЛ')), km:ol.sphere.getDistance(center, [d.lon, d.lat])/1000}})).sort((a,b)=>(a.f.price+a.km*5)-(b.f.price+b.km*5)).slice(0,3);
    
    document.getElementById('finderResult').innerHTML = cands.map(x=>`<div><b>${{x.d.name}}</b>: ${{x.f.price}}₽ (${{x.km.toFixed(0)}}км)</div>`).join('');
}}

function applyFilters() {{
    const sp=document.getElementById('flt_spimex').checked, luk=document.getElementById('flt_lukoil').checked;
    vectorSource.getFeatures().forEach(f=>{{
        const d=f.get('data'), vis=(d.company==='spimex'&&sp)||(d.company==='lukoil'&&luk);
        f.setStyle(vis ? getMarkerStyle(d) : new ol.style.Style({{display:'none'}}));
    }});
}}

function doSearch() {{
    const q=document.getElementById('search').value.toLowerCase(), f=vectorSource.getFeatures().find(x=>x.get('data').name.toLowerCase().includes(q));
    if(f) {{ map.getView().animate({{center:f.getGeometry().getCoordinates(), zoom:10}}); showPopup(f.get('data'), f.getGeometry().getCoordinates()); }}
}}

initMap();
</script></body></html>"""

# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────
def main():
    print(f"🚀 Запуск AMN v1.3 [Hybrid Parser]...")
    gen_time = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    
    otp_prices = load_otp_prices()
    ref_stations = load_stations_reference(STATIONS_CSV)
    luk_stations = load_lukoil_stations()
    
    latest_pdf = download_latest_spimex()
    if not latest_pdf:
        local_pdfs = sorted(glob.glob(os.path.join(DATA_DIR, "oil_*.pdf")))
        if local_pdfs: latest_pdf = local_pdfs[-1]
    spimex_markers = process_spimex_data(latest_pdf, ref_stations)
    
    lukoil_markers = []
    for f in fetch_lukoil_xlsx():
        lukoil_markers.extend(process_lukoil_xlsx(f, luk_stations))
    
    all_markers = spimex_markers + lukoil_markers
    html = render_openlayers_html(all_markers, get_file_date_short(latest_pdf) if latest_pdf else "?", gen_time, otp_prices)
    
    with open(os.path.join(OUTPUT_DIR, "index.html"), "w", encoding="utf-8") as f: f.write(html)
    print(f"✅ Готово! SPIMEX={len(spimex_markers)}, LUKOIL={len(lukoil_markers)}")

if __name__ == "__main__":
    main()
