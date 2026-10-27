"""Render the production Jinja/OpenLayers template from synthetic markers."""
import json
from pathlib import Path
import shutil
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from jinja2 import Environment, FileSystemLoader
from src.utils import save_data_to_csv
from src.config import Config

def generate():
    out = ROOT / "output"
    out.mkdir(exist_ok=True)
    Config.OUTPUT_DIR = str(out)
    markers = json.loads((ROOT / "demo/stations.json").read_text(encoding="utf-8"))
    for marker in markers:
        marker["company"] = {"rosneft": "lukoil_term", "other": "spimex"}.get(marker["company"], marker["company"])
        marker["company_segment"] = marker["company"]
        marker["categories"] = sorted({fuel["cat"] for fuel in marker["fuels"]})
    save_data_to_csv(markers, "demo-prices.csv")
    context = dict(gen_time="DEMO 27.10.2026", date_str="DEMO", markers_json=json.dumps(markers, ensure_ascii=False), otp_json="{}", providers_json=json.dumps({"osm": {"title": "OpenStreetMap", "url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png", "visible": True}}), TARIFF_PER_KM=170, TARIFF_PER_TON_KM=7, TRUCK_TONS=25, MAP_CENTER_LAT=56.82, MAP_CENTER_LON=60.6, MAP_ZOOM_START=8)
    html = Environment(loader=FileSystemLoader(str(ROOT / "templates"))).get_template("index.html").render(context)
    html = html.replace("<body>", '<body><div style="position:fixed;bottom:0;left:0;right:0;z-index:10000;background:#fff0b5;padding:10px;text-align:center">ДЕМО · Названия и цены вымышлены · Источники прайсов не вызываются · <a href="demo-prices.csv">CSV</a></div>')
    (out / "index.html").write_text(html, encoding="utf-8")
    shutil.copytree(ROOT / "src/js", out / "js", dirs_exist_ok=True)
    return out / "index.html"

if __name__ == "__main__":
    print(generate())
