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
# 1. НАСТРОЙКИ И КОНФИГУРАЦИЯ
# ──────────────────────────────────────────────────────────────────────────────
DATA_DIR = os.getenv("DATA_DIR", "pdf_data")
# Важно: output для совместимости с вашим сервером
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")

STATIONS_CSV = os.path.join(DATA_DIR, "stations.csv")
LUKOIL_STATIONS_CSV = os.path.join(DATA_DIR, "stations_lukoil.csv")
OTP_FILE = os.path.join(DATA_DIR, "stavkiOTP.txt")

# Логистика
TARIFF_PER_KM = float(os.getenv("TARIFF_PER_KM", "170"))
TARIFF_PER_TON_KM = float(os.getenv("TARIFF_PER_TON_KM", "7"))
TRUCK_TONS = float(os.getenv("TRUCK_TONS", "25"))

# Карта
MAP_PROVIDER = os.getenv("MAP_PROVIDER", "yandex_map")
MAP_CENTER_LAT = float(os.getenv("MAP_CENTER_LAT", "55.75"))
MAP_CENTER_LON = float(os.getenv("MAP_CENTER_LON", "37.62"))
MAP_ZOOM_START = int(os.getenv("MAP_ZOOM_START", "5"))

# Источники
SPIMEX_URL = "https://spimex.com/markets/oil_products/trades/results/"
SPIMEX_BASE_URL = "https://spimex.com"
LUKOIL_PRICE_URL = "https://auto.lukoil.ru/ru/ForBusiness/wholesale/price"
LUKOIL_BASE_URL = "https://auto.lukoil.ru"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Классификатор
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
# 2. УТИЛИТЫ (ПАРСИНГ, ОЧИСТКА)
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
# 3. ЗАГРУЗКА СПРАВОЧНИКОВ
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
# 4. ЛОГИКА ПАРСИНГА (HYBRID PARSER)
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
        
        # 1. Попытка найти ШИРОКИЙ формат (Wide)
        wide_header_idx = -1
        fuel_map = {} 
        
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
            
            col_st_idx = -1
            known_norms = set(stations_df["name_norm"])
            for c in df_raw.columns:
                if c in fuel_map: continue
                sample = df_raw[c].astype(str).head(30).tolist()
                match_count = sum(1 for x in sample if re.sub(r'\W+', '', x.lower()) in known_norms)
                if match_count > 0: col_st_idx = c; break
            if col_st_idx == -1: col_st_idx = 0
            
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
            print("  -> Detected LONG format (Fallback)")
            
            col_st_idx = -1
            col_pr_idx = -1
            col_fl_idx = -1
            
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
                    
                    if not cat: 
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
        
        fuels = []
        seen = set()
        cats = set()
        override_segs = set()
        
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

# ──────────────────────────────────────────────────────────────────────────────
# 5. ГЕНЕРАЦИЯ HTML (БОГАТЫЙ ИНТЕРФЕЙС)
# ──────────────────────────────────────────────────────────────────────────────
def render_openlayers_html(markers, date_str, gen_time, otp_prices):
    markers_json = json.dumps(markers, ensure_ascii=False)
    otp_json = json.dumps(otp_prices, ensure_ascii=False)
    providers = {
        "yandex_map": {"title": "Yandex Карта", "url": "https://core-renderer-tiles.maps.yandex.net/tiles?l=map&v=23.09.14-0&x={x}&y={y}&z={z}&scale=1&lang=ru_RU", "visible": MAP_PROVIDER=="yandex_map"},
        "yandex_sat": {"title": "Yandex Спутник", "url": "https://core-sat-renderer-tiles.maps.yandex.net/tiles?l=sat&v=3.888.0&x={x}&y={y}&z={z}&lang=ru_RU", "visible": MAP_PROVIDER=="yandex_sat"},
        "osm": {"title": "OpenStreetMap", "url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png", "visible": MAP_PROVIDER in ["openstreetmap", "osm"]}
    }
    
    return f"""<!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>Карта нефтебаз AMN v1.6</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ol@v9.2.4/ol.css"/>
    <style>
        body, html, #map {{ margin: 0; width: 100%; height: 100%; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; overflow: hidden; }}
        
        /* SIDE PANEL */
        .panel {{ position: fixed; top: 10px; left: 10px; z-index: 1000; background: rgba(255,255,255,0.95); width: 320px; max-height: 90vh; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.2); display: flex; flex-direction: column; transition: transform 0.3s; }}
        .panel.closed {{ transform: translateX(-340px); }}
        .panel-header {{ background: #222; color: #fff; padding: 12px; border-radius: 12px 12px 0 0; display: flex; justify-content: space-between; align-items: center; cursor: pointer; }}
        .panel-body {{ overflow-y: auto; padding: 10px; flex: 1; }}
        .toggle-btn {{ position: absolute; left: 330px; top: 10px; background: #222; color: #fff; border: none; padding: 10px; border-radius: 50%; cursor: pointer; display: none; box-shadow: 0 2px 10px rgba(0,0,0,0.2); }}
        .panel.closed + .toggle-btn {{ display: block; }}
        
        /* UPDATE BADGE */
        .update-badge {{ position: fixed; top: 20px; left: 50%; transform: translateX(-50%); z-index: 9999; background: #28a745; color: white; padding: 12px 24px; border-radius: 30px; font-weight: bold; box-shadow: 0 6px 20px rgba(0,0,0,0.4); opacity: 1; transition: opacity 1s; pointer-events: none; border: 2px solid white; }}
        
        /* CONTROLS */
        .section {{ margin-bottom: 15px; border: 1px solid #eee; padding: 10px; border-radius: 8px; background: #fff; }}
        .section-title {{ font-weight: bold; font-size: 13px; margin-bottom: 8px; text-transform: uppercase; color: #555; }}
        input, select, button {{ width: 100%; padding: 8px; margin-top: 4px; box-sizing: border-box; border: 1px solid #ccc; border-radius: 6px; font-size: 12px; }}
        button {{ background: #222; color: #fff; border: none; cursor: pointer; font-weight: bold; }}
        button:hover {{ background: #444; }}
        button.secondary {{ background: #fff; color: #222; border: 1px solid #222; }}
        .checkbox-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 5px; font-size: 12px; }}
        
        /* POPUP */
        .ol-popup {{ position: absolute; background-color: white; box-shadow: 0 1px 4px rgba(0,0,0,0.2); padding: 15px; border-radius: 10px; bottom: 45px; left: -50px; min-width: 280px; z-index: 5000; border: 1px solid #ccc; }}
        .ol-popup:after, .ol-popup:before {{ top: 100%; border: solid transparent; content: " "; height: 0; width: 0; position: absolute; pointer-events: none; }}
        .ol-popup:after {{ border-top-color: white; border-width: 10px; left: 48px; margin-left: -10px; }}
        .ol-popup:before {{ border-top-color: #cccccc; border-width: 11px; left: 48px; margin-left: -11px; }}
        
        /* MODAL */
        .modal {{ display: none; position: fixed; z-index: 2000; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.5); align-items: center; justify-content: center; }}
        .modal-content {{ background-color: #fefefe; padding: 20px; border-radius: 10px; width: 500px; max-height: 80vh; overflow-y: auto; }}
        .otp-row {{ display: flex; justify-content: space-between; margin-bottom: 5px; border-bottom: 1px solid #eee; padding: 5px 0; }}
        .otp-input {{ width: 80px; text-align: right; }}
        
        /* FINDER */
        .best-price-row {{ background: #d4edda; border: 1px solid #c3e6cb; padding: 5px; margin-top: 5px; border-radius: 4px; font-size: 11px; }}
    </style>
</head>
<body>

<div class="update-badge" id="updateBadge">✅ КАРТА ОБНОВЛЕНА: {gen_time}</div>

<div class="panel" id="panel">
    <div class="panel-header" onclick="document.getElementById('panel').classList.toggle('closed')">
        <span>AMN v1.6</span><span>✕</span>
    </div>
    <div class="panel-body">
        <div style="font-size: 11px; color: #777; margin-bottom: 10px;">
            Данные от: {date_str}<br>Обновлено: {gen_time}
        </div>

        <div class="section">
            <div class="section-title">Фильтры</div>
            <div class="checkbox-grid">
                <label><input type="checkbox" id="flt_spimex" checked onchange="applyFilters()"> SPIMEX</label>
                <label><input type="checkbox" id="flt_lukoil" checked onchange="applyFilters()"> Лукойл</label>
            </div>
            <div style="margin: 8px 0; border-top: 1px solid #eee;"></div>
            <div class="checkbox-grid">
                <label><input type="checkbox" id="flt_benz" checked onchange="applyFilters()"> Бензин</label>
                <label><input type="checkbox" id="flt_dtl" checked onchange="applyFilters()"> ДтЛ</label>
                <label><input type="checkbox" id="flt_dte" checked onchange="applyFilters()"> ДтЕ</label>
                <label><input type="checkbox" id="flt_dtz" checked onchange="applyFilters()"> ДтЗ</label>
                <label><input type="checkbox" id="flt_dta" checked onchange="applyFilters()"> ДтА</label>
                <label><input type="checkbox" id="flt_sug" checked onchange="applyFilters()"> СУГ</label>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Логистика (OSRM)</div>
            <div style="display: flex; gap: 5px;">
                <input type="number" id="tariff" value="{TARIFF_PER_KM}" placeholder="Р/км">
                <input type="number" id="tonnage" value="{TRUCK_TONS}" placeholder="Тонн">
            </div>
            <div id="routeStatus" style="margin-top: 8px; font-size: 11px; color: #555; background: #f9f9f9; padding: 5px; border-radius: 4px;">
                Выберите станцию для расчета
            </div>
            <div style="display: flex; gap: 5px; margin-top: 5px;">
                <button class="secondary" onclick="clearRoute()">Сброс</button>
                <button onclick="buildRouteManually()">Построить</button>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Поиск лучшей цены</div>
            <div style="font-size: 11px; color: #666; margin-bottom: 5px;"><b>Alt + Клик</b> на карте для поиска в радиусе</div>
            <div style="display: flex; gap: 5px;">
                <input type="number" id="finderRadius" value="300" placeholder="Радиус (км)">
                <select id="finderFuel">
                    <option value="ДтЛ">ДтЛ</option>
                    <option value="ДтЕ">ДтЕ</option>
                    <option value="ДтЗ">ДтЗ</option>
                    <option value="ДтА">ДтА</option>
                    <option value="Бензин">Бензин</option>
                    <option value="СУГ">СУГ</option>
                </select>
            </div>
            <div id="finderResult" style="margin-top: 5px; font-size: 11px;"></div>
        </div>

        <div class="section">
            <div class="section-title">Настройки</div>
            <button class="secondary" onclick="document.getElementById('otpModal').style.display='flex'">Надбавки ОТП</button>
            <select id="mapProvider" onchange="changeMapLayer()" style="margin-top: 5px;">
                <option value="yandex_map">Yandex Карта</option>
                <option value="yandex_sat">Yandex Спутник</option>
                <option value="osm">OpenStreetMap</option>
            </select>
        </div>
        
        <input type="text" id="search" placeholder="Поиск станции..." onkeydown="if(event.key==='Enter') doSearch()">
    </div>
</div>
<button class="toggle-btn" onclick="document.getElementById('panel').classList.toggle('closed')">☰</button>

<div id="map"></div>
<div id="popup" class="ol-popup">
    <a href="#" id="popup-closer" style="position: absolute; top: 2px; right: 8px; text-decoration: none; color: #999; font-size: 20px;">✖</a>
    <div id="popup-content"></div>
</div>

<div id="otpModal" class="modal">
    <div class="modal-content">
        <h3>Надбавки ОТП (Руб/Т)</h3>
        <div style="margin-bottom: 10px; font-size: 12px; color: #666;">Налив + Хранение (5 дней)</div>
        <div id="otpList"></div>
        <div style="margin-top: 15px; display: flex; gap: 10px;">
            <button onclick="saveOtp()">Сохранить</button>
            <button class="secondary" onclick="document.getElementById('otpModal').style.display='none'">Отмена</button>
        </div>
    </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/ol@v9.2.4/dist/ol.js"></script>
<script>
    // --- ДАННЫЕ ---
    const markers = {markers_json};
    let otpPrices = {otp_json};
    const providers = {json.dumps(providers, ensure_ascii=False)};
    
    // --- ГЛОБАЛЬНЫЕ ---
    let map, vectorSource, routeSource, overlay;
    let activeStation = null;
    let routePoints = [];

    // --- СТИЛИ МАРКЕРОВ ---
    function getMarkerStyle(m) {{
        let color = '#d32f2f'; // Default Red
        let iconChar = '💧';
        
        if (m.company_segment === 'lukoil_spot') {{ color = '#e11d48'; iconChar = '💧'; }} // Red
        else if (m.company_segment === 'lukoil_term') {{ color = '#2563eb'; iconChar = '💧'; }} // Blue
        else if (m.company_segment === 'spimex') {{ color = '#0ea5e9'; iconChar = '🏛'; }} // Light Blue
        
        const svg = `<svg width="32" height="42" viewBox="0 0 32 42" xmlns="http://www.w3.org/2000/svg">
            <filter id="s"><feDropShadow dx="0" dy="2" stdDeviation="2" flood-color="rgba(0,0,0,0.3)"/></filter>
            <path d="M16 40 Q16 40 9 28 A 14 14 0 1 1 23 28 Q16 40 16 40 Z" fill="${{color}}" stroke="white" stroke-width="1.5" filter="url(#s)"/>
            <circle cx="16" cy="14" r="8.5" fill="white"/>
            <text x="16" y="19.5" font-size="13" text-anchor="middle" font-family="Segoe UI Emoji, Arial">${{iconChar}}</text>
        </svg>`;
        
        return new ol.style.Style({{
            image: new ol.style.Icon({{
                src: 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svg.trim()),
                anchor: [0.5, 1],
                scale: 1
            }})
        }});
    }}

    function getFuelColor(cat) {{
        const map = {{'Бензин':'red', 'ДтЛ':'blue', 'ДтЕ':'orange', 'ДтЗ':'lightblue', 'ДтА':'teal', 'СУГ':'purple'}};
        return map[cat] || 'gray';
    }}

    function getOtpSurcharge(name) {{
        const n = name.toLowerCase();
        for (let k in otpPrices) {{
            if (n.includes(k)) return otpPrices[k].nalyv + otpPrices[k].storage;
        }}
        return 0;
    }}

    // --- ИНИЦИАЛИЗАЦИЯ КАРТЫ ---
    function initMap() {{
        const layers = [];
        for (let k in providers) {{
            const p = providers[k];
            const layer = new ol.layer.Tile({{
                source: new ol.source.XYZ({{ url: p.url, crossOrigin: 'anonymous' }}),
                visible: p.visible,
                properties: {{ name: k }}
            }});
            layers.push(layer);
        }}

        vectorSource = new ol.source.Vector();
        routeSource = new ol.source.Vector();
        
        markers.forEach(m => {{
            const feature = new ol.Feature({{
                geometry: new ol.geom.Point(ol.proj.fromLonLat([m.lon, m.lat])),
                data: m
            }});
            feature.setStyle(getMarkerStyle(m));
            vectorSource.addFeature(feature);
        }});

        map = new ol.Map({{
            target: 'map',
            layers: [
                ...layers,
                new ol.layer.Vector({{ source: routeSource }}),
                new ol.layer.Vector({{ source: vectorSource }})
            ],
            view: new ol.View({{
                center: ol.proj.fromLonLat([{MAP_CENTER_LON}, {MAP_CENTER_LAT}]),
                zoom: {MAP_ZOOM_START}
            }})
        }});

        const container = document.getElementById('popup');
        overlay = new ol.Overlay({{
            element: container,
            autoPan: true,
            autoPanAnimation: {{ duration: 250 }}
        }});
        map.addOverlay(overlay);

        document.getElementById('popup-closer').onclick = function() {{ overlay.setPosition(undefined); return false; }};

        map.on('singleclick', function(evt) {{
            if (evt.originalEvent.altKey) {{
                handleFinderClick(evt.coordinate);
                return;
            }}

            const feature = map.forEachFeatureAtPixel(evt.pixel, f => f);
            
            if (feature && feature.get('data')) {{
                showPopup(feature.get('data'), evt.coordinate);
            }} else if (activeStation) {{
                addRoutePoint(evt.coordinate, evt.originalEvent);
            }} else {{
                overlay.setPosition(undefined);
            }}
        }});
        
        // Init OTP inputs
        let h=''; for(let k in otpPrices) h+=`<div class="otp-row"><span>${{k}}</span><div><input class="otp-input" id="on_${{k}}" value="${{otpPrices[k].nalyv}}"><input class="otp-input" id="os_${{k}}" value="${{otpPrices[k].storage}}"></div></div>`;
        document.getElementById('otpList').innerHTML=h;

        // Auto-hide badge after 5s
        setTimeout(() => {{
            const b = document.getElementById('updateBadge');
            if(b) b.style.opacity = '0';
        }}, 5000);

        applyFilters();
    }}

    // --- POPUP ---
    function showPopup(d, coords) {{
        const content = document.getElementById('popup-content');
        const otp = getOtpSurcharge(d.name);
        const otpBadge = otp > 0 ? `<div style='background:#fff3cd; padding:3px; font-size:10px; margin-bottom:5px; border-radius:4px; border:1px solid #ffeeba;'>ОТП Надбавка: <b>+${{otp}}</b> ₽</div>` : '';

        let rows = '';
        d.fuels.forEach(f => {{
            const price = f.price + otp;
            const color = getFuelColor(f.cat);
            rows += `<tr>
                <td style='border-left: 4px solid ${{color}}; padding-left: 8px; padding-bottom:4px;'>${{f.name}}</td>
                <td style='text-align:right; font-weight:bold; color:#0056b3;'>${{price.toLocaleString()}} ₽</td>
            </tr>`;
        }});

        content.innerHTML = `
            <div style="font-size:14px; font-weight:bold; margin-bottom:2px;">${{d.name}}</div>
            <div style="font-size:11px; color:#666; margin-bottom:8px;">${{d.company==='spimex'?'СПб Биржа':'Лукойл'}}</div>
            ${{otpBadge}}
            <table style="width:100%; font-size:12px; border-collapse: collapse;">${{rows}}</table>
            <button style="margin-top:10px; padding:6px; width:100%; background:#222; color:white; border:none; border-radius:4px; cursor:pointer;" onclick="startLogistics('${{d.code}}')">🚚 Расчет логистики</button>
        `;
        overlay.setPosition(coords);
    }}

    // --- LOGISTICS ---
    function ensureRoutingCompatibleLayer() {{
        const selector = document.getElementById('mapProvider');
        if (!selector || selector.value === 'osm') return false;
        selector.value = 'osm';
        changeMapLayer();
        return true;
    }}

    function startLogistics(code) {{
        const feature = vectorSource.getFeatures().find(f => f.get('data').code === code);
        if(!feature) return;
        activeStation = feature.get('data');
        routePoints = [];
        routeSource.clear();
        const switchedToOsm = ensureRoutingCompatibleLayer();
        document.getElementById('routeStatus').innerHTML = switchedToOsm
            ? `<b>${{activeStation.name}}</b><br>Слой переключен на OSM для точного дорожного маршрута. Кликайте по карте для точек доставки...`
            : `<b>${{activeStation.name}}</b><br>Кликайте по карте для точек доставки...`;
        document.getElementById('popup-closer').click();
    }}

    function addRoutePoint(coord, originalEvent) {{
        const lonLat = ol.proj.toLonLat(coord);
        routePoints.push(lonLat);
        
        const pt = new ol.Feature({{ geometry: new ol.geom.Point(coord) }});
        pt.setStyle(new ol.style.Style({{ image: new ol.style.Circle({{ radius: 6, fill: new ol.style.Fill({{color:'#f59e0b'}}), stroke: new ol.style.Stroke({{color:'#fff', width:2}}) }}) }}));
        routeSource.addFeature(pt);
        
        if (originalEvent && originalEvent.shiftKey) buildRouteManually();
        else document.getElementById('routeStatus').innerHTML = `Точек: ${{routePoints.length}}. Shift+Click для расчета.`;
    }}

    async function buildRouteManually() {{
        if (!activeStation || routePoints.length === 0) return;
        
        const start = [activeStation.lon, activeStation.lat];
        const coords = [start, ...routePoints];
        const coordStr = coords.map(c => `${{c[0]}},${{c[1]}}`).join(';');
        
        document.getElementById('routeStatus').innerHTML = "Расчет OSRM...";
        
        try {{
            const resp = await fetch(`https://router.project-osrm.org/route/v1/driving/${{coordStr}}?overview=full&geometries=geojson`);
            const json = await resp.json();
            
            if (json.routes && json.routes.length) {{
                const r = json.routes[0];
                const km = r.distance / 1000;
                const tariff = parseFloat(document.getElementById('tariff').value);
                const tons = parseFloat(document.getElementById('tonnage').value);
                const cost = Math.round(km * tariff);
                const costPerTon = Math.round(cost / tons);
                
                const format = new ol.format.GeoJSON();
                const feature = format.readFeature(r.geometry, {{
                    dataProjection: 'EPSG:4326', featureProjection: 'EPSG:3857'
                }});
                feature.setStyle(new ol.style.Style({{ stroke: new ol.style.Stroke({{ color: '#2563eb', width: 4 }}) }}));
                routeSource.getFeatures().forEach(f => {{
                    if (f.getGeometry() instanceof ol.geom.LineString) routeSource.removeFeature(f);
                }});
                routeSource.addFeature(feature);
                
                document.getElementById('routeStatus').innerHTML = `
                    Дистанция: <b>${{km.toFixed(1)}} км</b><br>
                    Рейс: <b>${{cost.toLocaleString()}} ₽</b><br>
                    На тонну: <b style="color:green">+${{costPerTon}} ₽</b>
                `;
            }}
        }} catch(e) {{
            document.getElementById('routeStatus').innerHTML = "Ошибка OSRM";
        }}
    }}
    
    function clearRoute() {{
        routeSource.clear();
        routePoints = [];
        activeStation = null;
        document.getElementById('routeStatus').innerHTML = "Выберите станцию";
    }}

    // --- FINDER (BEST PRICE) ---
    async function handleFinderClick(coord) {{
        const center = ol.proj.toLonLat(coord);
        const radius = parseFloat(document.getElementById('finderRadius').value);
        const fuelType = document.getElementById('finderFuel').value;
        const tariff = parseFloat(document.getElementById('tariff').value);
        const tons = parseFloat(document.getElementById('tonnage').value);
        
        const candidates = [];
        vectorSource.getFeatures().forEach(f => {{
            if (f.getStyle() === null) return; // Skip hidden
            const d = f.get('data');
            // Haversine
            const dx = (d.lon - center[0]) * Math.cos((d.lat + center[1])/2 * Math.PI/180);
            const dy = (d.lat - center[1]);
            const distDeg = Math.sqrt(dx*dx + dy*dy);
            const km = distDeg * 111.32;
            
            if (km > radius) return;
            
            const fuel = d.fuels.find(x => x.cat === fuelType || (x.cat==='ДтЛ' && fuelType==='ДтЛ')); 
            if (fuel) {{
                const otp = getOtpSurcharge(d.name);
                const estDeliv = (km * 1.3) * tariff / tons;
                candidates.push({{ data: d, fuel: fuel, km_est: km, total_est: fuel.price + otp + estDeliv, otp: otp }});
            }}
        }});
        
        candidates.sort((a,b) => a.total_est - b.total_est);
        const top3 = candidates.slice(0, 3);
        
        const resDiv = document.getElementById('finderResult');
        resDiv.innerHTML = "Уточнение маршрутов...";
        
        let html = "";
        for (let c of top3) {{
            try {{
                const url = `https://router.project-osrm.org/route/v1/driving/${{c.data.lon}},${{c.data.lat}};${{center[0]}},${{center[1]}}?overview=false`;
                const resp = await fetch(url);
                const json = await resp.json();
                const realKm = json.routes[0].distance / 1000;
                const deliv = (realKm * tariff) / tons;
                const total = c.fuel.price + c.otp + deliv;
                
                html += `<div class="best-price-row">
                    <b>${{c.data.name}}</b> (${{realKm.toFixed(0)}} км)<br>
                    База: ${{c.fuel.price}} | Дост: +${{Math.round(deliv)}}<br>
                    Итого: <b style="color:green; font-size:13px;">${{Math.round(total)}} ₽/т</b>
                </div>`;
            }} catch(e) {{}}
        }}
        resDiv.innerHTML = html || "Ничего не найдено";
        
        routeSource.clear();
        const dest = new ol.Feature({{ geometry: new ol.geom.Point(coord) }});
        dest.setStyle(new ol.style.Style({{ image: new ol.style.Circle({{ radius: 6, fill: new ol.style.Fill({{color:'#dc3545'}}), stroke: new ol.style.Stroke({{color:'#fff', width:2}}) }}) }}));
        routeSource.addFeature(dest);
    }}

    // --- UTILS ---
    function changeMapLayer() {{
        const val = document.getElementById('mapProvider').value;
        map.getLayers().forEach(l => {{
            if (l.get('properties') && l.get('properties').name) l.setVisible(l.get('properties').name === val);
        }});
    }}
    
    function applyFilters() {{
        const fSpimex = document.getElementById('flt_spimex').checked;
        const fLukoil = document.getElementById('flt_lukoil').checked;
        const fuels = {{
            'Бензин': document.getElementById('flt_benz').checked,
            'ДтЛ': document.getElementById('flt_dtl').checked,
            'ДтЕ': document.getElementById('flt_dte').checked,
            'ДтЗ': document.getElementById('flt_dtz').checked,
            'ДтА': document.getElementById('flt_dta').checked,
            'СУГ': document.getElementById('flt_sug').checked,
        }};
        
        vectorSource.getFeatures().forEach(f => {{
            const d = f.get('data');
            let vis = false;
            if (d.company === 'spimex' && fSpimex) vis = true;
            if (d.company === 'lukoil' && fLukoil) vis = true;
            
            if (vis) {{
                const hasFuel = d.categories.some(c => fuels[c]);
                if (!hasFuel) vis = false;
            }}
            
            if (!vis) f.setStyle(new ol.style.Style({{ display: 'none' }})); 
            else f.setStyle(getMarkerStyle(d));
        }});
    }}

    function doSearch() {{
        const q = document.getElementById('search').value.toLowerCase();
        const f = vectorSource.getFeatures().find(f => {{
            const d = f.get('data');
            return d.name.toLowerCase().includes(q) || d.code.toLowerCase().includes(q);
        }});
        if (f) {{
            map.getView().animate({{ center: f.getGeometry().getCoordinates(), zoom: 10 }});
            showPopup(f.get('data'), f.getGeometry().getCoordinates());
        }} else {{
            alert("Не найдено");
        }}
    }}
    
    function saveOtp() {{
        for (let k in otpPrices) {{
            otpPrices[k].nalyv = parseFloat(document.getElementById(`on_${{k}}`).value) || 0;
            otpPrices[k].storage = parseFloat(document.getElementById(`os_${{k}}`).value) || 0;
        }}
        document.getElementById('otpModal').style.display = 'none';
        alert("Надбавки применены! Цены пересчитаны.");
    }}

    initMap();
</script>
</body>
</html>"""

# ──────────────────────────────────────────────────────────────────────────────
# 6. MAIN
# ──────────────────────────────────────────────────────────────────────────────
def main():
    print(f"🚀 Запуск AMN v1.6 [Fix: output + markers]...")
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
    
    out_file = os.path.join(OUTPUT_DIR, "index.html")
    with open(out_file, "w", encoding="utf-8") as f: f.write(html)
    
    print(f"✅ Готово! Карта сохранена в: {os.path.abspath(out_file)}")
    print(f"📊 Маркеров: SPIMEX={len(spimex_markers)}, LUKOIL={len(lukoil_markers)}")

if __name__ == "__main__":
    main()
