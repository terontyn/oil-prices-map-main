import os
import glob
import json
import re
from urllib.request import urlopen, Request
from collections import defaultdict

import pdfplumber
import pandas as pd
from curl_cffi import requests as crequests
from bs4 import BeautifulSoup

from src.config import Config
from src.utils import (
    normalize_code, parse_coordinates, clean_price, 
    get_fuel_category, get_file_date_short
)
from src.logger import logger

def load_otp_prices():
    prices = {k: {"nalyv": 0, "storage": 0} for k in Config.OTP_STATION_KEYS}
    if not os.path.exists(Config.OTP_FILE): 
        logger.debug(f"Файл OTP не найден: {Config.OTP_FILE}")
        return prices
    try:
        with open(Config.OTP_FILE, encoding="utf-8") as f:
            content = f.read().strip()
            if content.startswith('{'):
                data = json.loads(content)
                for k in Config.OTP_STATION_KEYS:
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
        logger.error(f"Ошибка чтения OTP справочника: {e}", exc_info=True)
    return prices

def load_stations_reference(csv_path):
    if not os.path.exists(csv_path): 
        logger.warning(f"Справочник станций не найден: {csv_path}")
        return pd.DataFrame()
    
    try:
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
    except Exception as e:
        logger.error(f"Ошибка загрузки справочника станций: {e}")
        return pd.DataFrame()

def _norm_name(s):
    """Нормализует название нефтебазы для нечёткого сопоставления."""
    s = str(s).lower().strip()
    s = re.sub(r'\(.*?\)', '', s)
    s = re.sub(r'[«»"\'.,]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def _name_score(a, b):
    """Оценка сходства двух строк (0–1) на основе коэффициента Жаккара."""
    if not a or not b: return 0.0
    if a == b: return 1.0
    ta, tb = set(a.split()), set(b.split())
    if not ta or not tb: return 0.0
    jaccard = len(ta & tb) / len(ta | tb)
    contains = 0.15 if (a in b or b in a) else 0.0
    return min(1.0, jaccard + contains)

def load_lukoil_stations():
    if not os.path.exists(Config.LUKOIL_STATIONS_CSV): 
        return pd.DataFrame()
    try:
        df = pd.read_csv(Config.LUKOIL_STATIONS_CSV, encoding="utf-8-sig", dtype=str)
    except:
        try: df = pd.read_csv(Config.LUKOIL_STATIONS_CSV, encoding="cp1251", dtype=str)
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
    # Используем новую нормализацию
    out["name_norm"] = out["name"].apply(_norm_name)
    return out.dropna(subset=["lat", "lon"])

def match_lukoil_station(name, stations_df, threshold=0.55):
    """Находит нефтебазу через Fuzzy Matching"""
    if stations_df.empty or not name: return None
    norm = _norm_name(name)
    best_score = -1
    best_row = None
    
    for _, row in stations_df.iterrows():
        row_norm = row.get("name_norm", _norm_name(row["name"]))
        score = _name_score(norm, row_norm)
        if score > best_score:
            best_score = score
            best_row = row
            
    if best_score >= threshold:
        return best_row
    return None

def download_latest_spimex():
    logger.info("SPIMEX: Поиск свежего бюллетеня...")
    try:
        r = crequests.get(Config.SPIMEX_URL, impersonate="chrome116", timeout=20)
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            if 'oil_' in href.lower() and '.pdf' in href.lower():
                pdf_url = Config.SPIMEX_BASE_URL + href if href.startswith('/') else href
                logger.info(f"SPIMEX: Скачивание {pdf_url}")
                pdf_resp = crequests.get(pdf_url, impersonate="chrome116", timeout=30)
                out_path = os.path.join(Config.DATA_DIR, "latest_spimex.pdf")
                with open(out_path, 'wb') as f:
                    f.write(pdf_resp.content)
                return out_path
    except Exception as e:
        logger.warning(f"Не удалось скачать SPIMEX: {e}")
    return None

def process_spimex_data(pdf_file, stations_df):
    if not pdf_file or stations_df.empty: return []
    date_str = get_file_date_short(pdf_file)
    results = []
    
    try:
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
    except Exception as e:
        logger.error(f"Ошибка парсинга PDF {pdf_file}: {e}", exc_info=True)

    return aggregate_markers_by_coordinates(results, company="spimex")

def fetch_lukoil_xlsx():
    try:
        headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "ru-RU"}
        req = Request(Config.LUKOIL_PRICE_URL, headers=headers)
        html = urlopen(req, timeout=15).read().decode("utf-8", errors="replace")
        xlsx_paths = re.findall(r'href=["\']?(/FileSystem/[^"\'>\s]+\.xlsx[^"\'>\s]*)', html, re.I)
        downloaded = []
        for path in xlsx_paths:
            url = Config.LUKOIL_BASE_URL + path
            if "dl=" not in url: url += "&dl=1" if "?" in url else "?dl=1"
            fname = os.path.basename(path).split('?')[0]
            if not fname.endswith('.xlsx'): fname += '.xlsx'
            
            if "spot" in path.lower() or "spot" in fname.lower(): prefix = "lukoil_spot_"
            elif "term" in path.lower() or "term" in fname.lower(): prefix = "lukoil_term_"
            else: prefix = "lukoil_"
                
            save_path = os.path.join(Config.DATA_DIR, prefix + fname)
            if not os.path.exists(save_path):
                logger.info(f"LUKOIL: Скачивание {fname}")
                with open(save_path, "wb") as f:
                    f.write(urlopen(Request(url, headers=headers), timeout=30).read())
            downloaded.append(save_path)
    except Exception as e:
        logger.warning(f"Ошибка скачивания Лукойл: {e}")

    files = glob.glob(os.path.join(Config.DATA_DIR, "*lukoil*.xlsx"))
    logger.info(f"LUKOIL: Найдено файлов: {len(files)}")
    return files

def process_lukoil_xlsx(xlsx_file, stations_df):
    if not os.path.exists(xlsx_file): return []
    file_date = get_file_date_short(xlsx_file)
    results = []
    
    fname = os.path.basename(xlsx_file).lower()
    file_seg = "lukoil_spot" if "spot" in fname else "lukoil_term" if "term" in fname else "lukoil_other"
    logger.debug(f"Обработка {os.path.basename(xlsx_file)} ({file_seg})...")

    try:
        df_raw = pd.read_excel(xlsx_file, header=None, engine="openpyxl")
        df_raw = df_raw.dropna(how="all").reset_index(drop=True)
        
        wide_header_idx = -1
        fuel_map = {} 
        
        # Попытка найти Wide формат
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
            logger.debug(f"  -> WIDE формат (Заголовок: строка {wide_header_idx})")
            
            col_st_idx = -1
            known_norms = set(stations_df["name_norm"])
            for c in df_raw.columns:
                if c in fuel_map: continue
                sample = df_raw[c].astype(str).head(30).tolist()
                match_count = sum(1 for x in sample if _norm_name(x) in known_norms)
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
                if match is None and last_valid_match is not None: match = last_valid_match
                    
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
            logger.debug("  -> LONG формат (Fallback)")
            
            col_st_idx = -1
            col_pr_idx = -1
            col_fl_idx = -1
            
            known_norms = set(stations_df["name_norm"])
            best_match_count = 0
            for c in df_raw.columns:
                sample = df_raw[c].astype(str).head(50).tolist()
                cnt = sum(1 for x in sample if _norm_name(x) in known_norms)
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
                if match is None and last_valid_match is not None: match = last_valid_match
                
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
        logger.error(f"Ошибка чтения {xlsx_file}: {e}", exc_info=True)
        
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
        
        fuels.sort(key=lambda x: (Config.CAT_ORDER.index(x["cat"]) if x["cat"] in Config.CAT_ORDER else 999, x["price"]))
        
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
