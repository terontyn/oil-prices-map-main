import os

class Config:
    DATA_DIR = os.getenv("DATA_DIR", "data")
    OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
    
    STATIONS_CSV = os.path.join(DATA_DIR, "stations.csv")
    LUKOIL_STATIONS_CSV = os.path.join(DATA_DIR, "stations_lukoil.csv")
    OTP_FILE = os.path.join(DATA_DIR, "stavkiOTP.txt")
    
    TARIFF_PER_KM = float(os.getenv("TARIFF_PER_KM", "170"))
    TARIFF_PER_TON_KM = float(os.getenv("TARIFF_PER_TON_KM", "7"))
    TRUCK_TONS = float(os.getenv("TRUCK_TONS", "25"))
    
    MAP_PROVIDER = os.getenv("MAP_PROVIDER", "osm")
    MAP_CENTER_LAT = float(os.getenv("MAP_CENTER_LAT", "55.75"))
    MAP_CENTER_LON = float(os.getenv("MAP_CENTER_LON", "37.62"))
    MAP_ZOOM_START = int(os.getenv("MAP_ZOOM_START", "5"))
    
    SPIMEX_URL = "https://spimex.com/markets/oil_products/trades/results/"
    SPIMEX_BASE_URL = "https://spimex.com"
    LUKOIL_PRICE_URL = "https://auto.lukoil.ru/ru/ForBusiness/wholesale/price"
    LUKOIL_BASE_URL = "https://auto.lukoil.ru"

    # КЛАССИФИКАЦИЯ: "евро" убрано из Бензина
    FUEL_TYPES = {
        'Бензин': ['бензин', 'аи-92', 'аи-95', 'аи-98', 'аи-100', 'регуляр', 'премиум', 'аи'],
        'ДтА': ['дт-а', 'класс 4', 'вид 4', 'арктич', 'минус 44', 'минус 45', 'минус 50', 'минус 52', 'дта'],
        'ДтЗ': ['дт-з', 'класс 0', 'класс 1', 'класс 2', 'класс 3', 'зимн', 'минус 20', 'минус 26', 'минус 32', 'минус 35', 'минус 38', 'дтз'],
        'ДтЕ': ['дт-е', 'сорт e', 'сорт е', 'сорт f', 'минус 15'],
        'ДтЛ': ['дт-л', 'сорт c', 'сорт с', 'сорт d', 'летн', 'дтл'],
        'СУГ': ['суг', 'газ', 'пропан', 'бутан', 'lpg', 'сжиж']
    }
    
    # Порядок проверки: сначала проверяем специфичный дизель, потом общие категории
    CAT_CHECK_ORDER = ["ДтА", "ДтЗ", "ДтЕ", "ДтЛ", "Бензин", "СУГ"]
    CAT_ORDER = ["Бензин", "ДтЛ", "ДтЕ", "ДтЗ", "ДтА", "СУГ"]

    OTP_STATION_KEYS = [
        'лпдс воронеж', 'лпдс володарская', 'нс солнечногорская', 'нс нагорная', 
        'лпдс сокур', 'лпдс невская', 'лпдс черкассы', 'лпдс никольское-1', 
        'нп брянск', 'лпдс белгород'
    ]

    @staticmethod
    def init_dirs():
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
