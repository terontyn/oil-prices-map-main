import time
import datetime
import traceback
from main import main

if __name__ == "__main__":
    print("🚀 Инициализация системы мониторинга...")
    while True:
        print(f"\n[{datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')}] Запуск обновления карты...")
        try:
            main()
        except Exception as e:
            print(f"❌ Ошибка во время выполнения:\n{traceback.format_exc()}")
        
        print("💤 Обновление завершено. Ожидание 1 час...")
        time.sleep(3600) # Пауза 3600 секунд (1 час)
