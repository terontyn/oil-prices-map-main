import os
import json
import glob
import shutil
import time
import datetime
import schedule
from jinja2 import Environment, FileSystemLoader

from src.config import Config
from src.logger import setup_logging
from src.parsers import (
    load_otp_prices, 
    load_stations_reference, 
    load_lukoil_stations,
    download_latest_spimex,
    process_spimex_data,
    fetch_lukoil_xlsx,
    process_lukoil_xlsx
)
from src.utils import get_file_date_short, get_msk_time_str, save_data_to_csv

def render_template(context):
    env = Environment(loader=FileSystemLoader("templates"))
    template = env.get_template("index.html")
    return template.render(context)

def update_map_job():
    logger = setup_logging()
    logger.info("⚡ Начинаем обновление карты (Job started)...")
    
    try:
        Config.init_dirs()
        gen_time = get_msk_time_str()

        # 1. Загрузка данных
        otp_prices = load_otp_prices()
        ref_stations = load_stations_reference(Config.STATIONS_CSV)
        luk_stations = load_lukoil_stations()

        # 2. SPIMEX
        latest_pdf = download_latest_spimex()
        if not latest_pdf:
            local_pdfs = sorted(glob.glob(os.path.join(Config.DATA_DIR, "oil_*.pdf")))
            if local_pdfs: latest_pdf = local_pdfs[-1]

        spimex_markers = process_spimex_data(latest_pdf, ref_stations)

        # 3. LUKOIL
        lukoil_files = fetch_lukoil_xlsx()
        lukoil_markers = []
        for f in lukoil_files:
            lukoil_markers.extend(process_lukoil_xlsx(f, luk_stations))

        all_markers = spimex_markers + lukoil_markers

        # === ЭКСПОРТ В CSV ===
        try:
            # Имя файла: prices_20260305_1405.csv
            now_str = datetime.datetime.now().strftime('%Y%m%d_%H%M')
            csv_name = f"prices_{now_str}.csv"
            report_path = save_data_to_csv(all_markers, csv_name)
            if report_path:
                logger.info(f"📊 Отчет сохранен: {os.path.basename(report_path)}")
        except Exception as e:
            logger.error(f"Ошибка экспорта CSV: {e}")
        # =====================

        # 4. Контекст для HTML
        providers_config = {
            "yandex_map": {"title": "Yandex Карта", "url": "https://core-renderer-tiles.maps.yandex.net/tiles?l=map&v=23.09.14-0&x={x}&y={y}&z={z}&scale=1&lang=ru_RU", "visible": Config.MAP_PROVIDER=="yandex_map"},
            "yandex_sat": {"title": "Yandex Спутник", "url": "https://core-sat-renderer-tiles.maps.yandex.net/tiles?l=sat&v=3.888.0&x={x}&y={y}&z={z}&lang=ru_RU", "visible": Config.MAP_PROVIDER=="yandex_sat"},
            "osm": {"title": "OpenStreetMap", "url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png", "visible": Config.MAP_PROVIDER in ["openstreetmap", "osm"]}
        }
        
        context = {
            "gen_time": gen_time,
            "date_str": get_file_date_short(latest_pdf) if latest_pdf else "?",
            "markers_json": json.dumps(all_markers, ensure_ascii=False),
            "otp_json": json.dumps(otp_prices, ensure_ascii=False),
            "providers_json": json.dumps(providers_config, ensure_ascii=False),
            "TARIFF_PER_KM": Config.TARIFF_PER_KM,
            "TARIFF_PER_TON_KM": Config.TARIFF_PER_TON_KM,
            "TRUCK_TONS": Config.TRUCK_TONS,
            "MAP_CENTER_LAT": Config.MAP_CENTER_LAT,
            "MAP_CENTER_LON": Config.MAP_CENTER_LON,
            "MAP_ZOOM_START": Config.MAP_ZOOM_START
        }

        # 5. Копирование статики
        static_src = os.path.join("src", "js")
        static_dst = os.path.join(Config.OUTPUT_DIR, "js")
        if os.path.exists(static_dst): shutil.rmtree(static_dst)
        shutil.copytree(static_src, static_dst)

        # 6. Рендеринг HTML
        html_content = render_template(context)
        out_file = os.path.join(Config.OUTPUT_DIR, "index.html")
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(html_content)
            
        logger.info(f"✅ Обновление завершено! Время (МСК): {gen_time}")
        
    except Exception as e:
        logger.critical(f"❌ Сбой обновления: {e}", exc_info=True)

def main():
    logger = setup_logging()
    logger.info("🚀 Сервис AMN запущен. Расписание: ежедневно в 14:05 МСК.")
    
    update_map_job()
    
    schedule.every().day.at("14:05").do(update_map_job)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
