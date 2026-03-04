# -*- coding: utf-8 -*-

import os
import glob
import json
import datetime
import re
from pathlib import Path
from collections import defaultdict

import pdfplumber
import pandas as pd
import folium
import shutil

# ──────────────────────────────────────────────────────────────────────────────
# НАСТРОЙКИ
# ──────────────────────────────────────────────────────────────────────────────
DATA_FOLDER = os.getenv("DATA_DIR", "pdf_data")
STATIONS_CSV = os.path.join(DATA_FOLDER, "stations.csv")
LUKOIL_XLSX = os.path.join(DATA_FOLDER, "lukoil.xlsx")

TARIFF_PER_KM = float(os.getenv("TARIFF_PER_KM", "170"))
TARIFF_PER_TON_KM = float(os.getenv("TARIFF_PER_TON_KM", "7"))
MAP_PROVIDER = os.getenv("MAP_PROVIDER", "yandex_map")
MAP_CENTER_LAT = float(os.getenv("MAP_CENTER_LAT", "55.75"))
MAP_CENTER_LON = float(os.getenv("MAP_CENTER_LON", "37.62"))
MAP_ZOOM_START = int(os.getenv("MAP_ZOOM_START", "5"))

os.makedirs(DATA_FOLDER, exist_ok=True)

FUEL_TYPES = {
    'Бензин': ['бензин', 'аи-92', 'аи-95', 'аи-98', 'аи-100', 'регуляр', 'премиум', 'euro', 'евро', 'аи'],
    'ДтА': ['дт-а', 'класс 4', 'вид 4', 'арктич', 'минус 44', 'минус 45', 'минус 50', 'минус 52', 'дта'],
    'ДтЗ': ['дт-з', 'класс 0', 'класс 1', 'класс 2', 'класс 3', 'зимн', 'минус 20', 'минус 26', 'минус 32', 'минус 35', 'минус 38', 'дтз'],
    'ДтЕ': ['дт-е', 'сорт e', 'сорт е', 'сорт f'],
    'ДтЛ': ['дт-л', 'сорт c', 'сорт с', 'сорт d', 'летн', 'минус 5', 'минус 10', 'дтл'],
    'СУГ': ['суг', 'газ', 'пропан', 'бутан', 'lpg', 'сжиж']
}

_CAT_ORDER = ["Бензин", "ДтЛ", "ДтЕ", "ДтЗ", "ДтА", "СУГ"]

BASEMAP_PROVIDERS = {
    "yandex_map": {
        "tiles": "https://core-renderer-tiles.maps.yandex.net/tiles?l=map&v=23.09.14-0&x={x}&y={y}&z={z}&scale=1&lang=ru_RU",
        "attr": "&copy; Yandex",
        "name": "Yandex Карта",
    },
    "yandex_sat": {
        "tiles": "https://core-sat-renderer-tiles.maps.yandex.net/tiles?l=sat&v=3.888.0&x={x}&y={y}&z={z}&lang=ru_RU",
        "attr": "&copy; Yandex",
        "name": "Yandex Спутник",
    },
    "openstreetmap": {
        "tiles": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
        "attr": "&copy; OpenStreetMap contributors",
        "name": "OpenStreetMap",
    },
}

def normalize_code(code):
    if code is None: return ""
    code = str(code).upper().strip().replace('"', "").replace("\n", "")
    repls = {"А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T", "Х": "X", "У": "Y"}
    return "".join([repls.get(c, c) for c in code])

def parse_coordinates(coord_str):
    try:
        return float(str(coord_str).replace(",", ".").strip())
    except (TypeError, ValueError):
        return None

def clean_price(value):
    try:
        s = str(value).strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
        v = float(s)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None

def format_price(price):
    if price is None: return "—"
    return f"{price:,.0f}".replace(",", " ") + " ₽"

def get_fuel_category(fuel_name):
    if pd.isna(fuel_name) or not fuel_name: return None
    s = str(fuel_name).lower()
    for cat, keywords in FUEL_TYPES.items():
        if any(kw in s for kw in keywords): return cat
    return None

def add_map_layers(map_obj):
    selected_provider = MAP_PROVIDER
    if selected_provider not in BASEMAP_PROVIDERS:
        selected_provider = "yandex_map"
        print(f"⚠️ MAP_PROVIDER={MAP_PROVIDER!r} не поддерживается. Используем {selected_provider!r}.")

    for provider_key, provider in BASEMAP_PROVIDERS.items():
        folium.TileLayer(
            tiles=provider["tiles"],
            attr=provider["attr"],
            name=provider["name"],
            overlay=False,
            control=True,
            show=(provider_key == selected_provider),
        ).add_to(map_obj)

def get_fuel_style(fuel_type):
    cat = get_fuel_category(fuel_type)
    mapping = {"Бензин": ("red", "fire"), "ДтЛ": ("blue", "tint"), "ДтЕ": ("orange", "tint"), "ДтЗ": ("lightblue", "tint"), "ДтА": ("cadetblue", "snowflake"), "СУГ": ("purple", "leaf")}
    return mapping.get(cat, ("gray", "info-sign"))

def aggregate_markers_by_coordinates(markers, precision=5):
    buckets = defaultdict(list)
    for m in markers:
        if m.get("lat") and m.get("lon"): buckets[(round(float(m["lat"]), precision), round(float(m["lon"]), precision))].append(m)

    out = []
    for (lat_r, lon_r), items in buckets.items():
        if len(items) == 1: out.append(items[0]); continue
        names = [str(x.get("name", "")).strip() for x in items if str(x.get("name", "")).strip()]
        uniq_names = []
        for n in names:
            if n not in uniq_names: uniq_names.append(n)
        merged_name = "Нефтебаза" if not uniq_names else (uniq_names[0] if len(uniq_names) == 1 else " / ".join(uniq_names[:2]) + (" …" if len(uniq_names) > 2 else ""))

        fuels, seen, cats = [], set(), set()
        for it in items:
            for f in (it.get("fuels") or []):
                fname, fprice = str(f.get("name", "")).strip(), f.get("price", None)
                if (fname.lower(), fprice) not in seen:
                    seen.add((fname.lower(), fprice))
                    fuels.append({"name": fname, "price": fprice, "cat": f.get("cat") or get_fuel_category(fname)})
                    cat = f.get("cat") or get_fuel_category(fname)
                    if cat: cats.add(cat)

        order_index = {c: i for i, c in enumerate(_CAT_ORDER)}
        fuels.sort(key=lambda x: (order_index.get(x.get("cat"), 999), str(x.get("name", "")).lower()))
        cat_list = sorted(list(cats), key=lambda c: _CAT_ORDER.index(c) if c in _CAT_ORDER else 999)
        color, icon = get_fuel_style(fuels[0]["name"]) if len(cat_list) == 1 else ("gray", "info-sign")

        out.append({
            "code": "PT_{:.5f}_{:.5f}".format(lat_r, lon_r).replace(".", "_"),
            "name": merged_name, "lat": float(items[0]["lat"]), "lon": float(items[0]["lon"]),
            "fuels": fuels, "color": color, "icon": icon, "categories": cat_list
        })
    return out

def create_popup_html(data, company):
    safe_name = str(data["name"]).replace("'", "\\'").replace('"', '&quot;')
    comp_name = "Петербургская биржа" if company == "spimex" else "Лукойл"

    html = f"""<div style="font-family: Arial, sans-serif; width: 280px; font-size: 13px;">
        <div style="font-size: 10px; color: #888; text-transform: uppercase; margin-bottom: 4px;">{comp_name}</div>
        <div style="font-weight: bold; font-size: 15px; margin-bottom: 8px; border-bottom: 1px solid #eee; padding-bottom: 5px;">{data["name"]}</div>
        <div style="max-height: 160px; overflow-y: auto; overflow-x: hidden; padding-right: 5px; margin-bottom: 10px;">
            <table style="width: 100%; border-collapse: collapse;">"""

    fuels = data.get("fuels", [])
    if not fuels: html += """<tr><td style="padding: 4px 0; color:#777;">Нет цен</td><td></td></tr>"""
    else:
        for fuel in fuels:
            color_hex = {'red':'#dc3545', 'blue':'#0d6efd', 'orange':'#fd7e14', 'lightblue':'#0dcaf0', 'cadetblue':'#20c997', 'purple':'#6f42c1', 'gray':'#adb5bd'}.get(get_fuel_style(fuel['name'])[0], '#adb5bd')
            html += f"""<tr style="border-bottom: 1px solid #f9f9f9;"><td style="padding: 4px 0;"><span style="color: {color_hex}; font-size: 14px; margin-right: 4px;">●</span>{fuel["name"]}</td>
                <td style="text-align: right; font-weight: bold; color: #007bff;">{format_price(fuel["price"])}</td></tr>"""

    html += f"""</table></div>
        <div id="delivery-{data["code"]}" style="background: #f8f9fa; padding: 10px; border: 1px dashed #ccc; text-align: center; border-radius: 6px;">
            <button class="delivery-btn" style="background: #111; color: #fff; border: none; padding: 8px 12px; font-size: 11px; font-weight: bold; cursor: pointer; font-family: Arial; width: 100%; border-radius: 4px;"
                    onclick="event.stopPropagation(); startDelivery('{data["code"]}', {data["lat"]}, {data["lon"]}, '{safe_name}')">🚚 РАССЧИТАТЬ ЛОГИСТИКУ</button>
        </div></div>"""
    return html

def create_ui(date_info, gen_time):
    return f"""
<div id="updateBadge" style="position: fixed; top: 15px; left: 50%; transform: translateX(-50%); z-index: 10000; background: #28a745; color: white; padding: 10px 20px; border-radius: 30px; font-family: monospace; font-size: 14px; font-weight: bold; box-shadow: 0 4px 15px rgba(0,0,0,0.3); transition: opacity 1s ease-out; pointer-events: none; border: 2px solid #fff;">
    ✅ КОД ОБНОВЛЕН: {gen_time}
</div>

<style>
  .leaflet-container {{ font-family: Arial, sans-serif !important; }}
  .mono-fab {{ position: fixed; z-index: 9999; top: 18px; left: 18px; width: 44px; height: 44px; background: #111; color: #fff; border-radius: 50%; display: flex; align-items: center; justify-content: center; cursor: pointer; box-shadow: 4px 4px 0 rgba(0,0,0,0.1); font-size: 20px; font-weight: bold; border: 2px solid #fff; display: none; user-select: none; }}
  .mono-panel {{ position: fixed; z-index: 9998; top: 18px; left: 18px; width: 290px; background: #fff; border: 1px solid #111; box-shadow: 8px 8px 0 rgba(0,0,0,0.06); border-radius: 12px; overflow: hidden; user-select: none; }}
  .mono-header {{ background: #111; color: #fff; padding: 10px 12px; font-size: 11px; letter-spacing: .08em; text-transform: uppercase; display: flex; align-items: center; justify-content: space-between; cursor: move; }}
  .mono-header-controls {{ display: flex; gap: 6px; }}
  .mono-toggle {{ cursor: pointer; font-weight: 700; padding: 2px 8px; border: 1px solid rgba(255,255,255,.25); border-radius: 999px; transition: 0.2s; }}
  .mono-toggle:hover {{ background: rgba(255,255,255,0.2); }}
  .mono-content {{ display: none; padding: 12px; font-size: 12px; border-top: 1px solid #111; }}
  .mono-muted {{ color:#666; font-size: 11px; line-height: 1.25; }}
  .mono-block {{ padding: 10px; border: 1px solid #eee; border-radius: 12px; margin-top: 10px; }}
  .mono-row {{ display:flex; gap:8px; }} .mono-row > * {{ flex:1; }}
  .mono-input {{ width: 100%; padding: 8px 10px; border: 1px solid #ddd; border-radius: 12px; outline: none; font-size: 12px; }}
  .mono-btn {{ background: #111; color: #fff; border: 0; padding: 8px 10px; border-radius: 12px; cursor: pointer; font-size: 11px; letter-spacing: .04em; text-transform: uppercase; }}
  .mono-btn--ghost {{ background: #fff !important; color:#111 !important; border:1px solid #111 !important; }}
  .mono-checks {{ display: grid; grid-template-columns: 1fr 1fr; gap: 6px 10px; margin-top: 8px; }}
  .mono-check {{ display:flex; align-items:center; gap: 6px; font-size: 12px; color:#111; }}
  .result-box {{ margin-top: 10px; padding: 10px; background: #f6f8ff; border: 1px dashed #cfd6ff; border-radius: 12px; font-size: 11px; }}
</style>

<div class="mono-fab" id="restoreBtn" onclick="restorePanel()">☰</div>
<div class="mono-panel" id="uiPanel">
  <div class="mono-header" id="uiHeader">
    <span>Меню</span>
    <div class="mono-header-controls">
      <span class="mono-toggle" id="uiToggle" onclick="togglePanel()">+</span>
      <span class="mono-toggle" onclick="minimizePanel()">×</span>
    </div>
  </div>
  <div class="mono-content" id="uiContent">
    <div class="mono-muted">Данные от: {date_info}</div>
    <div class="mono-block">
      <div style="font-weight:700; font-size:12px; margin-bottom:6px;">Поиск нефтебаз</div>
      <div class="mono-row"><input id="searchInput" class="mono-input" placeholder="Название..." /><button class="mono-btn" onclick="searchStation()">Найти</button></div>
    </div>
    <div class="mono-block">
      <div style="font-weight:700; font-size:12px; margin-bottom:6px;">Фильтры</div>
      <div class="mono-checks">
        <label class="mono-check"><input type="checkbox" id="flt_spimex" checked onchange="applyFilters()"> СПб биржа</label>
        <label class="mono-check"><input type="checkbox" id="flt_lukoil" checked onchange="applyFilters()"> Лукойл</label>
      </div>
      <div class="mono-checks" style="margin-top:10px;">
        <label class="mono-check"><input type="checkbox" id="flt_Benz" checked onchange="applyFilters()"> Бензин</label>
        <label class="mono-check"><input type="checkbox" id="flt_DtL" checked onchange="applyFilters()"> ДтЛ</label>
        <label class="mono-check"><input type="checkbox" id="flt_DtE" checked onchange="applyFilters()"> ДтЕ (Меж)</label>
        <label class="mono-check"><input type="checkbox" id="flt_DtZ" checked onchange="applyFilters()"> ДтЗ</label>
        <label class="mono-check"><input type="checkbox" id="flt_DtA" checked onchange="applyFilters()"> ДтА</label>
        <label class="mono-check"><input type="checkbox" id="flt_SUG" checked onchange="applyFilters()"> СУГ</label>
      </div>
    </div>
    <div class="mono-block">
      <div style="font-weight:700; font-size:12px; margin-bottom:6px;">Логистика (OSRM)</div>
      <div class="mono-row" style="margin-top:8px;">
        <button class="mono-btn mono-btn--ghost" onclick="clearLogistics()">Сбросить</button>
        <button class="mono-btn" onclick="clearRouteOnly()">Убрать маршрут</button>
      </div>
      <div id="calcStatus" class="result-box" style="margin-top:8px;">Выберите станцию для расчёта</div>
    </div>
  </div>
</div>

<script>
  setTimeout(() => {{
     const badge = document.getElementById('updateBadge');
     if(badge) badge.style.opacity = '0';
  }}, 8000);

  let mapInst = null;
  let activeDelivery = {{ code: null, lat: null, lon: null, name: null }};
  let routingControl = null;
  window.__LAST_ROUTE_RESULT__ = null; 

  function findMap() {{ for (let k in window) {{ if (k.startsWith('map_') && window[k] instanceof L.Map) {{ mapInst = window[k]; return true; }} }} return false; }}
  function togglePanel() {{ const c = document.getElementById('uiContent'); const t = document.getElementById('uiToggle'); const isOpen = c.style.display === 'block'; c.style.display = isOpen ? 'none' : 'block'; t.textContent = isOpen ? '+' : '–'; }}
  function minimizePanel() {{ document.getElementById('uiPanel').style.display = 'none'; document.getElementById('restoreBtn').style.display = 'flex'; }}
  function restorePanel() {{ document.getElementById('restoreBtn').style.display = 'none'; document.getElementById('uiPanel').style.display = 'block'; document.getElementById('uiContent').style.display = 'block'; document.getElementById('uiToggle').textContent = '–'; }}

  (function initDrag() {{
    const panel = document.getElementById('uiPanel'); const header = document.getElementById('uiHeader');
    let dragging = false, startX=0, startY=0, startLeft=0, startTop=0;
    header.addEventListener('mousedown', (e) => {{ dragging = true; const r = panel.getBoundingClientRect(); startX = e.clientX; startY = e.clientY; startLeft = r.left; startTop = r.top; panel.style.left = startLeft + 'px'; panel.style.top = startTop + 'px'; panel.style.right = 'auto'; panel.style.bottom = 'auto'; e.preventDefault(); }});
    window.addEventListener('mousemove', (e) => {{ if (!dragging) return; panel.style.left = (startLeft + (e.clientX - startX)) + 'px'; panel.style.top  = (startTop + (e.clientY - startY)) + 'px'; }});
    window.addEventListener('mouseup', () => dragging = false);
  }})();

  function searchStation() {{
    const q = (document.getElementById('searchInput').value || '').trim().toLowerCase();
    if (!q) return;
    const best = (window.__STATIONS__ || []).find(m => (m._stationCode||'').toLowerCase().includes(q) || (m._stationName||'').toLowerCase().includes(q));
    if (!best) return document.getElementById('calcStatus').innerHTML = 'Ничего не найдено';
    if (mapInst && !mapInst.hasLayer(best)) best.addTo(mapInst);
    mapInst.setView(best.getLatLng(), Math.max(mapInst.getZoom(), 10)); best.openPopup();
  }}

  function applyFilters() {{
    if (!mapInst) return;
    const comp = {{ spimex: document.getElementById('flt_spimex').checked, lukoil: document.getElementById('flt_lukoil').checked }};
    const fuel = {{ 'Бензин': document.getElementById('flt_Benz').checked, 'ДтЛ': document.getElementById('flt_DtL').checked, 'ДтЕ': document.getElementById('flt_DtE').checked, 'ДтЗ': document.getElementById('flt_DtZ').checked, 'ДтА': document.getElementById('flt_DtA').checked, 'СУГ': document.getElementById('flt_SUG').checked }};
    for (const mk of (window.__STATIONS__ || [])) {{
      const c = (mk._company || '').toLowerCase();
      let ok = (c && comp[c] !== false);
      if (ok) {{
        const cats = mk._categories || [];
        if (cats.length > 0) {{ ok = false; for (const cat of cats) {{ if (fuel[cat] === true) {{ ok = true; break; }} }} }}
      }}
      if (ok) {{ if (!mapInst.hasLayer(mk)) mk.addTo(mapInst); }} else {{ if (mapInst.hasLayer(mk)) mapInst.removeLayer(mk); }}
    }}
  }}

  function clearRouteOnly() {{ if (!mapInst) return; if (routingControl) {{ mapInst.removeControl(routingControl); routingControl = null; }} document.getElementById('calcStatus').innerHTML = 'Маршрут удалён.'; window.__LAST_ROUTE_RESULT__ = null; }}
  function clearLogistics() {{ clearRouteOnly(); activeDelivery = {{ code: null, lat: null, lon: null, name: null }}; mapInst.getContainer().style.cursor = ''; document.getElementById('calcStatus').innerHTML = 'Логистика сброшена.'; }}

  function startDelivery(code, lat, lon, name) {{
    setTimeout(() => {{
      activeDelivery = {{ code, lat, lon, name }}; mapInst.getContainer().style.cursor = 'crosshair';
      document.getElementById('calcStatus').innerHTML = '<b>Отправитель:</b><br>' + name + '<br><br><span style="color:#b00020; font-weight:bold;">Кликните точку назначения...</span>';
      if (routingControl) {{ mapInst.removeControl(routingControl); routingControl = null; }}
      if (document.getElementById('uiContent').style.display === 'none') togglePanel();
    }}, 50);
  }}

  function buildRoute(start, end) {{
    if (!window.L || !L.Routing) return;
    routingControl = L.Routing.control({{
      waypoints: [ L.latLng(start[0], start[1]), L.latLng(end[0], end[1]) ], routeWhileDragging: false, addWaypoints: false, draggableWaypoints: false, show: false, fitSelectedRoutes: true,
      lineOptions: {{ styles: [{{ color: '#007bff', weight: 4, opacity: 0.8, dashArray: '8, 8' }}] }}, router: L.Routing.osrmv1({{ serviceUrl: 'https://router.project-osrm.org/route/v1' }})
    }}).addTo(mapInst);

    routingControl.on('routesfound', function(e) {{
      const km = (e.routes[0].summary.totalDistance || 0) / 1000.0;
      const resHtml = `<div class="result-box"><b>Маршрут:</b> ${{km.toFixed(1)}} км<br><b>Рейс:</b> ${{Math.round(km * {TARIFF_PER_KM}).toLocaleString()}} ₽<br><b>На 1 тонну:</b> ${{Math.round(km * {TARIFF_PER_TON_KM}).toLocaleString()}} ₽</div>`;
      window.__LAST_ROUTE_RESULT__ = {{ code: activeDelivery.code, html: resHtml }};
      document.getElementById('calcStatus').innerHTML = '<b>Результат:</b>' + resHtml;
      const popDiv = document.getElementById('delivery-' + activeDelivery.code); if (popDiv) popDiv.innerHTML = resHtml;
      mapInst.getContainer().style.cursor = ''; activeDelivery.code = null;
    }});
  }}

  setTimeout(() => {{
    if (findMap()) {{
      mapInst.on('click', function(e) {{ if (activeDelivery.code) buildRoute([activeDelivery.lat, activeDelivery.lon], [e.latlng.lat, e.latlng.lng]); }});
      mapInst.on('popupopen', function(e) {{ if (window.__LAST_ROUTE_RESULT__) {{ const popDiv = document.getElementById('delivery-' + window.__LAST_ROUTE_RESULT__.code); if (popDiv) popDiv.innerHTML = window.__LAST_ROUTE_RESULT__.html; }} }});
      setTimeout(applyFilters, 800); document.getElementById('uiContent').style.display = 'block'; document.getElementById('uiToggle').textContent = '–';
    }}
  }}, 1500);
</script>
"""

def add_markers(map_obj, markers, group, company):
    for m_data in markers:
        popup = folium.Popup(create_popup_html(m_data, company), max_width=320)
        popup.options['closeOnClick'] = False 
        marker = folium.Marker([m_data["lat"], m_data["lon"]], popup=popup, icon=folium.Icon(color=m_data["color"], icon=m_data["icon"], prefix="fa"))
        marker.add_to(group)
        js_meta = f"""(function() {{ var count = 0; function init() {{ var mk = window['{marker.get_name()}']; if (mk) {{ mk._company = '{company}'; mk._categories = {json.dumps(m_data.get('categories', []), ensure_ascii=False)}; mk._stationCode = {json.dumps(m_data.get('code'), ensure_ascii=False)}; mk._stationName = {json.dumps(m_data.get('name'), ensure_ascii=False)}; window.__STATIONS__ = window.__STATIONS__ || []; if (!window.__STATIONS__.some(x => x._leaflet_id === mk._leaflet_id)) window.__STATIONS__.push(mk); }} else if (count < 60) {{ count++; setTimeout(init, 100); }} }} init(); }})();"""
        map_obj.get_root().script.add_child(folium.Element(js_meta))

def load_stations_reference(csv_path):
    if not os.path.exists(csv_path): return pd.DataFrame()
    df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str)
    stations = pd.DataFrame()
    stations["code"] = df.iloc[:,0].apply(normalize_code)
    stations["lat"] = df.iloc[:,1].apply(parse_coordinates)
    stations["lon"] = df.iloc[:,2].apply(parse_coordinates)
    stations["name"] = df.iloc[:,3].astype(str)
    stations["fuel_type"] = df.iloc[:,4].astype(str) if len(df.columns) > 4 else ""
    return stations.dropna(subset=["lat", "lon"])

def process_spimex_data(pdf_file, stations_df):
    if stations_df.empty: return []
    station_fuels = defaultdict(list)
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table: continue
            for row in table:
                if not row or len(row) < 5: continue
                code = normalize_code(row[0])
                price = clean_price(row[-3]) if len(row) >= 3 else None
                if price and code:
                    match = stations_df[stations_df["code"] == code]
                    if not match.empty:
                        actual_fuel_name = str(row[1]).strip() if len(row) > 1 and str(row[1]).strip() else match.iloc[0]["fuel_type"]
                        cat = get_fuel_category(actual_fuel_name)
                        if cat: station_fuels[code].append({"name": actual_fuel_name, "price": price, "cat": cat})
    results = []
    for code, fuels in station_fuels.items():
        if not fuels: continue
        s = stations_df[stations_df["code"] == code].iloc[0]
        clr, ico = get_fuel_style(fuels[0]["name"])
        results.append({"code": code, "name": s["name"], "lat": s["lat"], "lon": s["lon"], "fuels": fuels, "color": clr, "icon": ico, "categories": list({f["cat"] for f in fuels if f.get("cat")})})
    return aggregate_markers_by_coordinates(results)

def process_lukoil_xlsx(xlsx_file):
    if not os.path.exists(xlsx_file): return []
    df = pd.read_excel(xlsx_file, engine="openpyxl", dtype=str)
    results = []
    for i, row in df.iterrows():
        name, lat, lon = str(row.iloc[1]).strip(), parse_coordinates(row.iloc[2]), parse_coordinates(row.iloc[3])
        if not name or lat is None or lon is None: continue
        fuels, cats = [], set()
        for fc in df.columns[4:]:
            price = clean_price(row.get(fc))
            if price:
                cat = get_fuel_category(fc)
                if cat: cats.add(cat); fuels.append({"name": fc, "price": price, "cat": cat})
        if fuels:
            clr, ico = get_fuel_style(fuels[0]["name"])
            results.append({"code": f"LUKOIL_{i+1}", "name": name, "lat": lat, "lon": lon, "fuels": fuels, "color": clr, "icon": ico, "categories": list(cats)})
    return aggregate_markers_by_coordinates(results)



def render_openlayers_html(markers, date_str, gen_time):
    providers = {
        "yandex_map": {
            "title": "Yandex Карта",
            "url": BASEMAP_PROVIDERS["yandex_map"]["tiles"],
            "visible": MAP_PROVIDER == "yandex_map",
        },
        "yandex_sat": {
            "title": "Yandex Спутник",
            "url": BASEMAP_PROVIDERS["yandex_sat"]["tiles"],
            "visible": MAP_PROVIDER == "yandex_sat",
        },
        "openstreetmap": {
            "title": "OpenStreetMap",
            "url": BASEMAP_PROVIDERS["openstreetmap"]["tiles"],
            "visible": MAP_PROVIDER == "openstreetmap",
        },
    }
    if not any(p["visible"] for p in providers.values()):
        providers["yandex_map"]["visible"] = True

    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Карта нефтебаз</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ol@v9.2.4/ol.css" />
<style>
html,body,#map{{margin:0;height:100%;width:100%;font-family:Arial,sans-serif}}
.panel{{position:fixed;z-index:1000;top:12px;left:12px;background:#fff;border:1px solid #ccc;border-radius:14px;box-shadow:0 2px 14px rgba(0,0,0,.18);max-width:360px;width:calc(100vw - 24px);max-height:calc(100vh - 24px);overflow:auto}}
.panel-inner{{padding:12px}}
.hdr{{background:#111;color:#fff;padding:10px 12px;font-weight:700;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0}}
.block{{border:1px solid #ddd;border-radius:12px;padding:10px;margin-top:10px}}
.title{{font-weight:700;margin-bottom:8px;color:#444}}
.row{{display:flex;gap:8px;margin-top:8px}}
.row input,.row select{{flex:1;padding:7px 8px;border:1px solid #ccc;border-radius:10px}}
button{{padding:7px 10px;border:1px solid #111;border-radius:999px;background:#fff;cursor:pointer}}
.btn-dark{{background:#111;color:#fff}}
.checks{{display:grid;grid-template-columns:1fr 1fr;gap:4px;font-size:13px}}
.small{{font-size:12px;color:#666}}
.result{{margin-top:8px;font-size:12px;background:#f8f8ff;border:1px dashed #c7c7ff;padding:8px;border-radius:10px}}
#popup{{background:#fff;padding:10px;border-radius:10px;border:1px solid #ddd;min-width:260px;box-shadow:0 2px 10px rgba(0,0,0,.12)}}
</style></head><body>
<div class="panel" id="panel"><div class="hdr">МЕНЮ <button onclick="togglePanel()" style="background:#111;color:#fff;border-color:#fff">×</button></div><div class="panel-inner" id="panelInner">
<div class="small">Данные: {date_str}<br/>Обновлено: {gen_time}</div>
<div class="block"><div class="title">Фильтры</div>
<div class="small">Поставщики</div>
<div class="checks">
<label><input type="checkbox" id="flt_spimex" checked onchange="applyFilters()"/> СПб биржа</label>
<label><input type="checkbox" id="flt_lukoil_spot" checked onchange="applyFilters()"/> Лукойл Спот</label>
<label><input type="checkbox" id="flt_lukoil_term" checked onchange="applyFilters()"/> Лукойл Терм</label>
<label><input type="checkbox" id="flt_lukoil_other" checked onchange="applyFilters()"/> Другие</label>
</div>
<div class="small" style="margin-top:6px">Топливо</div>
<div class="checks">
<label><input type="checkbox" id="flt_Benz" checked onchange="applyFilters()"/> Бензин</label>
<label><input type="checkbox" id="flt_DtL" checked onchange="applyFilters()"/> ДтЛ</label>
<label><input type="checkbox" id="flt_DtE" checked onchange="applyFilters()"/> ДтЕ</label>
<label><input type="checkbox" id="flt_DtZ" checked onchange="applyFilters()"/> ДтЗ</label>
<label><input type="checkbox" id="flt_DtA" checked onchange="applyFilters()"/> ДтА</label>
<label><input type="checkbox" id="flt_SUG" checked onchange="applyFilters()"/> СУГ</label>
</div></div>

<div class="block"><div class="title">Параметры перевозки</div>
<div class="row"><input id="tariffKm" type="number" step="0.1" value="{TARIFF_PER_KM}"/><input id="tonnage" type="number" step="0.1" value="25"/></div>
<div class="small">Тариф, ₽/км и тоннаж, т</div>
</div>

<div class="block"><div class="title">Логистика (до 10 точек)</div>
<div class="small">1) Нажмите «РАССЧИТАТЬ ЛОГИСТИКУ» в попапе станции<br/>2) Кликайте на карту — добавляются точки маршрута<br/>3) Shift+клик или «Построить маршрут» — расчёт</div>
<div class="row"><button onclick="buildRoute()" class="btn-dark">Построить маршрут</button><button onclick="clearRouteOnly()">Убрать маршрут</button></div>
<div class="row"><button onclick="resetAll()">Сбросить всё</button></div>
<div class="result" id="calcStatus">Выберите станцию для расчёта</div>
</div>

<div class="block"><div class="title">Найти лучшую цену</div>
<div class="row"><input id="bestRadius" type="number" step="1" value="300"/><select id="bestFuel"><option>Бензин</option><option>ДтЛ</option><option>ДтЕ</option><option>ДтЗ</option><option>ДтА</option><option>СУГ</option></select></div>
<div class="row"><button onclick="enterBestPriceMode()" class="btn-dark">Alt+Click на карте</button><button onclick="exitBestPriceMode()">Выйти</button></div>
<div class="result" id="bestStatus">Режим не активен</div>
</div>

<div class="block"><div class="title">Поиск нефтебазы</div><div class="row"><input id="searchInput" placeholder="Название/код"/><button onclick="searchStation()">Найти</button></div></div>

<div class="block"><div class="title">Слой карты</div><div class="row"><select id="baseLayer"></select></div></div>
</div></div>
<div id="map"></div>
<div id="popup" style="display:none;position:absolute;z-index:1100"></div>
<script src="https://cdn.jsdelivr.net/npm/ol@v9.2.4/dist/ol.js"></script>
<script>
const providers = {json.dumps(providers, ensure_ascii=False)};
const markers = {json.dumps(markers, ensure_ascii=False)};
const fuelKeyMap = {{'Бензин':'Benz','ДтЛ':'DtL','ДтЕ':'DtE','ДтЗ':'DtZ','ДтА':'DtA','СУГ':'SUG'}};
const layerEntries = Object.entries(providers);
const baseLayers = layerEntries.map(([k,p]) => [k,new ol.layer.Tile({{visible:p.visible, source:new ol.source.XYZ({{url:p.url, crossOrigin:'anonymous'}})}})]);
const markerSource = new ol.source.Vector();
const routeSource = new ol.source.Vector();
const waypointSource = new ol.source.Vector();
let routeFeature = null;
let routeWaypoints = [];
let originPoint = null;
let bestPriceMode = false;

function getMainPrice(d) {{
  const vals = (d.fuels||[]).map(x => Number(x.price)).filter(v => Number.isFinite(v) && v > 0);
  if (!vals.length) return '—';
  return Math.round(Math.min(...vals)).toLocaleString('ru-RU');
}}
function markerColor(m) {{
  if (m.company_segment === 'lukoil_spot') return '#e11d48';
  if (m.company_segment === 'lukoil_term') return '#2563eb';
  if (m.company_segment === 'lukoil_other') return '#6b7280';
  return '#0ea5e9';
}}
function markerStyleFn(feature) {{
  if (feature.get('hidden')) return null;
  const m = feature.get('data');
  return new ol.style.Style({{
    image: new ol.style.Circle({{ radius: 8, fill: new ol.style.Fill({{color: markerColor(m)}}), stroke: new ol.style.Stroke({{color:'#fff', width:1.5}}) }}),
    text: new ol.style.Text({{ text: getMainPrice(m), offsetY: 16, font: 'bold 12px Arial', fill: new ol.style.Fill({{color:'#111'}}), backgroundFill: new ol.style.Fill({{color:'rgba(255,255,255,0.85)'}},), padding: [2,4,2,4] }})
  }});
}}

for (const m of markers) {{
  const f = new ol.Feature({{ geometry: new ol.geom.Point(ol.proj.fromLonLat([m.lon, m.lat])) }});
  f.set('data', m);
  f.set('hidden', false);
  markerSource.addFeature(f);
}}

const markerLayer = new ol.layer.Vector({{source: markerSource, style: markerStyleFn}});
const waypointLayer = new ol.layer.Vector({{source: waypointSource, style: new ol.style.Style({{image: new ol.style.Circle({{radius:5, fill:new ol.style.Fill({{color:'#f59e0b'}}), stroke:new ol.style.Stroke({{color:'#fff', width:1}})}})}})}});
const routeLayer = new ol.layer.Vector({{source: routeSource}});

const map = new ol.Map({{
  target:'map',
  layers:[...baseLayers.map(x=>x[1]), routeLayer, waypointLayer, markerLayer],
  view:new ol.View({{center:ol.proj.fromLonLat([{MAP_CENTER_LON}, {MAP_CENTER_LAT}]), zoom:{MAP_ZOOM_START}}})
}});

const popup = document.getElementById('popup');
const calcStatus = document.getElementById('calcStatus');
const bestStatus = document.getElementById('bestStatus');
const overlay = new ol.Overlay({{element: popup, autoPan: true, autoPanAnimation: {{duration: 180}}}});
map.addOverlay(overlay);

function togglePanel() {{
  const p = document.getElementById('panelInner');
  p.style.display = p.style.display === 'none' ? 'block' : 'none';
}}

const baseSelect = document.getElementById('baseLayer');
for (const [k,p] of layerEntries) {{
  const opt = document.createElement('option'); opt.value = k; opt.textContent = p.title; if (p.visible) opt.selected = true; baseSelect.appendChild(opt);
}}
baseSelect.addEventListener('change', () => {{
  for (const [k, layer] of baseLayers) layer.setVisible(k === baseSelect.value);
}});

function escapeJsText(s) {{ return String(s || '').replace(/'/g, "\'"); }}
function buildPopupHtml(d) {{
  const fuels = (d.fuels||[]).map(x => `<tr><td>${{x.name}}</td><td style="text-align:right">${{x.price || '—'}} ₽</td></tr>`).join('');
  return `<div><b>${{d.name}}</b><div class="small">${{d.company==='spimex'?'СПб Биржа':'Лукойл'}}</div><table style="width:100%;margin-top:6px">${{fuels||'<tr><td>Нет данных</td></tr>'}}</table><div style="margin-top:8px"><button class="btn-dark" onclick="startDelivery('${{escapeJsText(d.code)}}',${{d.lat}},${{d.lon}},'${{escapeJsText(d.name)}}')">РАССЧИТАТЬ ЛОГИСТИКУ</button></div></div>`;
}}

function addWaypointLonLat(lon, lat) {{
  if (!originPoint) return;
  if (routeWaypoints.length >= 10) {{ calcStatus.textContent = 'Лимит: 10 точек назначения'; return; }}
  routeWaypoints.push([lon, lat]);
  const wp = new ol.Feature({{ geometry: new ol.geom.Point(ol.proj.fromLonLat([lon, lat])) }});
  waypointSource.addFeature(wp);
  calcStatus.innerHTML = `<b>Точка отгрузки:</b> ${{originPoint.name}}<br/>Промежуточных точек: ${{routeWaypoints.length}}`;
}}

async function buildRoute() {{
  if (!originPoint || routeWaypoints.length === 0) {{ calcStatus.textContent = 'Сначала выберите станцию и точки назначения'; return; }}
  const coords = [[originPoint.lon, originPoint.lat], ...routeWaypoints];
  const coordStr = coords.map(c => `${{c[0]}},${{c[1]}}`).join(';');
  calcStatus.textContent = 'Строим маршрут через OSRM...';
  try {{
    const url = `https://router.project-osrm.org/route/v1/driving/${{coordStr}}?overview=full&geometries=geojson`;
    const resp = await fetch(url);
    const data = await resp.json();
    if (!data.routes || !data.routes.length) throw new Error('Маршрут не найден');
    const r = data.routes[0];
    const line = r.geometry.coordinates.map(c => ol.proj.fromLonLat(c));
    if (routeFeature) routeSource.removeFeature(routeFeature);
    routeFeature = new ol.Feature({{ geometry: new ol.geom.LineString(line) }});
    routeFeature.setStyle(new ol.style.Style({{ stroke: new ol.style.Stroke({{ color:'#2563eb', width:4 }}) }}));
    routeSource.addFeature(routeFeature);
    const km = r.distance / 1000;
    const tariff = Number(document.getElementById('tariffKm').value || 0);
    const tonnage = Number(document.getElementById('tonnage').value || 0);
    const tripCost = km * tariff;
    const total = tripCost * tonnage;
    calcStatus.innerHTML = `<b>От:</b> ${{originPoint.name}}<br/><b>Дистанция:</b> ${{km.toFixed(1)}} км<br/><b>Ставка:</b> ${{Math.round(tripCost).toLocaleString('ru-RU')}} ₽ за рейс<br/><b>Итого (${{tonnage.toFixed(1)}} т):</b> ${{Math.round(total).toLocaleString('ru-RU')}} ₽`;
  }} catch(e) {{
    calcStatus.textContent = 'Ошибка маршрутизации: ' + e.message;
  }}
}}

function clearRouteOnly() {{
  if (routeFeature) routeSource.removeFeature(routeFeature);
  routeFeature = null;
}}
function resetAll() {{
  clearRouteOnly();
  originPoint = null;
  routeWaypoints = [];
  waypointSource.clear();
  calcStatus.textContent = 'Сброшено. Выберите станцию для расчёта';
}}

function haversineKm(lat1, lon1, lat2, lon2) {{
  const R = 6371;
  const dLat = (lat2-lat1) * Math.PI/180;
  const dLon = (lon2-lon1) * Math.PI/180;
  const a = Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLon/2)**2;
  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}}
function catFromFuelName(name) {{
  const s = String(name||'').toLowerCase();
  if (s.includes('бензин') || s.includes('аи-')) return 'Бензин';
  if (s.includes('дт-л')) return 'ДтЛ';
  if (s.includes('дт-е')) return 'ДтЕ';
  if (s.includes('дт-з')) return 'ДтЗ';
  if (s.includes('дт-а')) return 'ДтА';
  if (s.includes('суг') || s.includes('газ')) return 'СУГ';
  return '';
}}

function enterBestPriceMode() {{ bestPriceMode = true; bestStatus.textContent = 'Режим активен: Alt+Click по карте'; }}
function exitBestPriceMode() {{ bestPriceMode = false; bestStatus.textContent = 'Режим не активен'; }}

map.on('singleclick', async (evt) => {{
  const ft = map.forEachFeatureAtPixel(evt.pixel, (feature, layer) => layer === markerLayer ? feature : null);
  const isAlt = evt.originalEvent && evt.originalEvent.altKey;
  const isShift = evt.originalEvent && evt.originalEvent.shiftKey;

  if (ft) {{
    const d = ft.get('data');
    popup.innerHTML = buildPopupHtml(d);
    overlay.setPosition(evt.coordinate);
    return;
  }}
  overlay.setPosition(undefined);

  if (bestPriceMode && isAlt) {{
    const c = ol.proj.toLonLat(evt.coordinate);
    const radius = Number(document.getElementById('bestRadius').value || 300);
    const fuel = document.getElementById('bestFuel').value;
    let best = null;
    for (const f of markerSource.getFeatures()) {{
      if (f.get('hidden')) continue;
      const d = f.get('data');
      const dist = haversineKm(c[1], c[0], d.lat, d.lon);
      if (dist > radius) continue;
      for (const fuelRow of (d.fuels || [])) {{
        if ((fuelRow.cat || catFromFuelName(fuelRow.name)) !== fuel) continue;
        const price = Number(fuelRow.price);
        if (!Number.isFinite(price) || price <= 0) continue;
        if (!best || price < best.price) best = {{name:d.name, price, dist}};
      }}
    }}
    bestStatus.innerHTML = best ? `Лучшая цена ${{fuel}}: <b>${{Math.round(best.price).toLocaleString('ru-RU')}} ₽</b><br/>${{best.name}}, ${{best.dist.toFixed(1)}} км` : 'Не найдено в заданном радиусе';
    return;
  }}

  if (originPoint) {{
    const c = ol.proj.toLonLat(evt.coordinate);
    addWaypointLonLat(c[0], c[1]);
    if (isShift) await buildRoute();
  }}
}});

map.getViewport().addEventListener('contextmenu', async (e) => {{
  e.preventDefault();
  if (!originPoint) return;
  const pixel = map.getEventPixel(e);
  const coord = map.getCoordinateFromPixel(pixel);
  const c = ol.proj.toLonLat(coord);
  addWaypointLonLat(c[0], c[1]);
  await buildRoute();
}});

window.startDelivery = function(code, lat, lon, name) {{
  originPoint = {{code, lat, lon, name}};
  routeWaypoints = [];
  waypointSource.clear();
  clearRouteOnly();
  calcStatus.innerHTML = `<b>Точка отгрузки:</b> ${{name}}<br/>Кликните на карте точки назначения (до 10).`;
  overlay.setPosition(undefined);
}}
window.buildRoute = buildRoute;
window.clearRouteOnly = clearRouteOnly;
window.resetAll = resetAll;
window.enterBestPriceMode = enterBestPriceMode;
window.exitBestPriceMode = exitBestPriceMode;

window.searchStation = function() {{
  const q = (document.getElementById('searchInput').value || '').trim().toLowerCase();
  if (!q) return;
  const found = markerSource.getFeatures().find(f => {{
    const d = f.get('data');
    return !f.get('hidden') && (((d.name||'').toLowerCase().includes(q)) || ((d.code||'').toLowerCase().includes(q)));
  }});
  if (!found) {{ calcStatus.textContent = 'Поиск: ничего не найдено'; return; }}
  map.getView().animate({{center: found.getGeometry().getCoordinates(), zoom: Math.max(map.getView().getZoom(), 8), duration: 250}});
}}

window.applyFilters = function() {{
  const supplier = {{
    spimex: document.getElementById('flt_spimex').checked,
    lukoil_spot: document.getElementById('flt_lukoil_spot').checked,
    lukoil_term: document.getElementById('flt_lukoil_term').checked,
    lukoil_other: document.getElementById('flt_lukoil_other').checked
  }};
  const fuel = {{Benz: document.getElementById('flt_Benz').checked, DtL: document.getElementById('flt_DtL').checked, DtE: document.getElementById('flt_DtE').checked, DtZ: document.getElementById('flt_DtZ').checked, DtA: document.getElementById('flt_DtA').checked, SUG: document.getElementById('flt_SUG').checked}};

  markerSource.forEachFeature((f) => {{
    const d = f.get('data');
    let okSupplier = supplier[d.company_segment || 'lukoil_other'] !== false;
    let okFuel = true;
    if (d.categories && d.categories.length) okFuel = d.categories.some(c => fuel[fuelKeyMap[c]] === true);
    f.set('hidden', !(okSupplier && okFuel));
  }});
  markerSource.changed();
}}
</script></body></html>"""


def main():
    # Получаем точное время генерации
    gen_time = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    
    ref = load_stations_reference(STATIONS_CSV)

    date_str = "неизвестно"
    spimex_markers = []
    pdfs = glob.glob(os.path.join(DATA_FOLDER, "*.pdf"))
    if pdfs and not ref.empty:
        latest = sorted(pdfs)[-1]
        try: date_str = datetime.datetime.strptime(os.path.basename(latest).split("_")[1][:8], "%Y%m%d").strftime("%d.%m.%Y")
        except: pass
        spimex_markers = process_spimex_data(latest, ref)

    lukoil_markers = process_lukoil_xlsx(LUKOIL_XLSX)
    for marker in spimex_markers:
        marker["company"] = "spimex"
        marker["company_segment"] = "spimex"
    for marker in lukoil_markers:
        marker["company"] = "lukoil"
        code_lower = str(marker.get("code", "")).lower()
        if "spot" in code_lower:
            marker["company_segment"] = "lukoil_spot"
        elif "term" in code_lower:
            marker["company_segment"] = "lukoil_term"
        else:
            marker["company_segment"] = "lukoil_other"
    html = render_openlayers_html(spimex_markers + lukoil_markers, date_str, gen_time)
    
    out_dir = Path(os.getenv("OUTPUT_DIR", "public"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "index.html"
    if out_file.is_dir(): shutil.rmtree(out_file)
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ Карта сохранена. Время обновления: {gen_time}")

if __name__ == "__main__":
    main()
