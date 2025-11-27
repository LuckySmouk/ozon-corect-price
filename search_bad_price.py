import os
import glob
import pandas as pd
from datetime import datetime
import re


def clean_price_value(value):
    """Очистка числовых значений от символов валюты и пробелов"""
    if pd.isna(value) or value is None or str(value).strip() == '':
        return 0.0

    cleaned = str(value).strip()
    cleaned = cleaned.replace('\u2009', '').replace(' ', '').replace('₽', '').replace('$', '').replace(',', '.')
    cleaned = ''.join(c for c in cleaned if c.isdigit() or c == '.')

    if not cleaned or cleaned == '.':
        return 0.0

    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def extract_datetime_from_filename(filename):
    """Извлечение даты из имени файла"""
    match = re.search(r'result_price_(\d{8}_\d{6})\.xlsx', filename)
    if match:
        dt_str = match.group(1)
        try:
            return datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
        except ValueError:
            pass
    return datetime.min


def find_latest_file(directory="out", prefix="result_price_", ext=".xlsx"):
    files = glob.glob(os.path.join(directory, f"{prefix}*{ext}"))
    if not files:
        raise FileNotFoundError(
            f"Файлы с префиксом '{prefix}' не найдены в директории '{directory}'")

    # Фильтрация файлов с валидными датами
    valid_files = []
    for f in files:
        dt = extract_datetime_from_filename(os.path.basename(f))
        if dt != datetime.min:
            valid_files.append((f, dt))
    
    if not valid_files:
        raise FileNotFoundError("Не найдено файлов с валидными датами")
    
    valid_files.sort(key=lambda x: x[1], reverse=True)
    return valid_files[0][0]


def calculate_deviation(price_1c, ozon_price):
    """Расчет процентного отклонения с учетом знака"""
    # Целевая цена = цена 1С + 15%
    target_price = price_1c * 1.10
    
    if target_price <= 0 or ozon_price <= 0:
        return None
    
    # Расчет процентного отклонения
    deviation = ((ozon_price - target_price) / target_price) * 100
    return deviation


def process_excel_file(file_path):
    df = pd.read_excel(file_path)

    required_columns = [
        "Ozon Product ID", "SKU", "Артикул",
        "Цена 1С", "Цена по карте озон",
        "Базовая цена API", "Старая цена API",
        "Минимальная цена API", "Название товара", "Ссылка на товар"
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Отсутствуют колонки: {', '.join(missing_cols)}")

    bad_prices = []

    for index, row in df.iterrows():
        try:
            # Извлечение и валидация данных
            ozon_id = str(row['Ozon Product ID']).strip()
            sku = str(int(float(row['SKU']))) if not pd.isna(row['SKU']) else "N/A"
            article = str(row['Артикул']).strip()
            
            # Обработка названия товара: берем только первое слово до пробела
            full_name = str(row['Название товара']).strip()
            first_space = full_name.find(' ')
            product_name = full_name[:first_space] if first_space != -1 else full_name
            
            product_url = str(row['Ссылка на товар']).strip()

            price_1c = clean_price_value(row['Цена 1С'])
            ozon_price = clean_price_value(row['Цена по карте озон'])

            base_price = clean_price_value(row['Базовая цена API'])
            old_price = clean_price_value(row['Старая цена API'])
            min_price = clean_price_value(row['Минимальная цена API'])

            # Пропускаем нулевые или отрицательные цены
            if price_1c <= 0 or ozon_price <= 0:
                continue

            dev = calculate_deviation(price_1c, ozon_price)

            # Проверяем абсолютное значение отклонения (> 3% в любую сторону)
            if dev is not None and abs(dev) > 3:
                # Форматируем отклонение со знаком
                sign = '+' if dev >= 0 else ''
                formatted_dev = f"{sign}{round(dev, 2)}%"
                
                bad_prices.append((
                    ozon_id, sku, article,
                    formatted_dev,
                    int(base_price), int(old_price),
                    int(min_price), int(price_1c),
                    int(ozon_price),
                    product_name,  # Используем обработанное название
                    product_url
                ))

        except Exception as e:
            print(f"Ошибка в строке {index}: {str(e)}")
            print(f"Сырые данные: {row.values}")
            continue

    return bad_prices


def save_bad_prices(data):
    """Сохранение результатов в файл"""
    if not os.path.exists('in'):
        os.makedirs('in')
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join("in", f"bad_price_{timestamp}.txt")

    with open(filename, 'w', encoding='utf-8') as f:
        for item in data:
            # Формат: OzonID SKU Артикул %Отклонение База Старая Минимум Цена1С ЦенаOzon Название Ссылка
            line = (f"{item[0]} {item[1]} {item[2]} {item[3]} {item[4]} {item[5]} "
                    f"{item[6]} {item[7]} {item[8]} {item[9]} {item[10]}\n")
            f.write(line)

    print(f"Найдено проблемных позиций: {len(data)}")
    print(f"Файл результатов: {filename}")


if __name__ == "__main__":
    try:
        input_file = find_latest_file()
        print(f"Обрабатываем файл: {input_file}")
        results = process_excel_file(input_file)

        if results:
            save_bad_prices(results)
        else:
            print("Товары с отклонениями не найдены")

    except Exception as e:
        print(f"Критическая ошибка: {str(e)}")