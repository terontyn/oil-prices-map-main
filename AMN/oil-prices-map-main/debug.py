import pdfplumber
import pandas as pd
import glob
import os

# Ищем PDF
files = glob.glob("data/*.pdf")
if not files:
    print("!!! Нет PDF файлов в папке data/")
    exit()

pdf_path = sorted(files)[-1]
print(f"Проверяем файл: {pdf_path}")

with pdfplumber.open(pdf_path) as pdf:
    page = pdf.pages[0] # Смотрим первую страницу
    table = page.extract_table()
    
    if not table:
        print("!!! Не удалось извлечь таблицу с первой страницы.")
        exit()

    print("\n--- ПЕРВЫЕ 10 СТРОК ТАБЛИЦЫ ---")
    for i, row in enumerate(table[:10]):
        print(f"Строка {i}: {row}")

    print("\n--- ПОПЫТКА НАЙТИ КОЛОНКИ ---")
    col_code = -1
    col_price = -1
    
    # Перебираем строки в поисках заголовка
    for i, row in enumerate(table[:10]):
        # Очищаем от None и переводим в нижний регистр для поиска
        row_clean = [str(x).lower() for x in row if x]
        print(f"Анализ строки {i}: {row_clean}")
        
        if 'код' in str(row_clean) and ('цена' in str(row_clean) or 'сделки' in str(row_clean)):
            print(f"!!! НАШЕЛ ЗАГОЛОВОК В СТРОКЕ {i} !!!")
            
            for idx, cell in enumerate(row):
                if not cell: continue
                val = cell.lower()
                if 'код' in val:
                    col_code = idx
                    print(f" -> Код инструмента в колонке {idx}")
                if 'цена' in val or 'сделки' in val:
                    col_price = idx
                    print(f" -> Цена в колонке {idx} ('{cell}')")
            break
            
    if col_code == -1 or col_price == -1:
        print("\n!!! НЕ НАШЕЛ КОЛОНКИ АВТОМАТИЧЕСКИ")
        print("Нужно посмотреть на 'ПЕРВЫЕ 10 СТРОК' выше и сказать мне, в каких колонках Код и Цена.")
    else:
        print(f"\nИТОГ: Код={col_code}, Цена={col_price}")
        print("Попробуйте найти эти колонки в данных:")
        # Пробуем вывести данные
        for row in table[i+1:i+5]:
            if len(row) > max(col_code, col_price):
                print(f"Code: {row[col_code]} | Price: {row[col_price]}")
