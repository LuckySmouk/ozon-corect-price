# conf.py


from typing import List, Tuple
import os
from dotenv import load_dotenv


# Конфигурация API
CLIENT_ID = os.getenv("OZON_CLIENT_ID")
API_KEY = os.getenv("OZON_API_KEY")
BASE_URL = "https://api-seller.ozon.ru"
HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json"
}

# Проверка, чтобы код не работал без ключей
if not CLIENT_ID or not API_KEY:
    raise ValueError("Не найдены OZON_CLIENT_ID и/или OZON_API_KEY в .env файле!")

# Настройки времени
FILE_CHECK_INTERVAL = 1900
PRODUCT_DELAY_RANGE = (3, 5) 
TIMEOUT = 5
MAX_API_ATTEMPTS = 3
PRICE_UPDATE_DELAY = 10
API_TIMEOUT = 10
BACKOFF_BASE = 2
BACKOFF_MAX = 60

# Общие настройки
THREADS_PER_PROXY = 3
MAX_PROXIES = 1
REQUEST_DELAY = (2, 4)
PROXY_CHANGE_DELAY = 1
MAX_RETRIES = 4
MAX_ATTEMPTS_PER_PRODUCT = 5
PRICE_TOLERANCE = 0.05
HTTPBIN_URL = "https://httpbin.org/ip"
SUPPORTED_SCHEMES: Tuple[str, ...] = ("http", "https")
MAX_FILE_AGE_MINUTES = 30

STATIC_USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.24 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.93 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 YaBrowser/20.7.3.100 Yowser/2.5 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 YaBrowser/21.5.2.644 Yowser/2.5 Safari/537.36"
    ]

PRICE_SELECTORS: List[str] = [
    "div[data-widget='webPrice'] button span"
]

PRICE_PATTERNS: List[str] = [  
    r'(\d+[\s.]?\d+)\s*[₽]',
    r'[\D](\d{1,3}(?:\s?\d{3})*(?:[.,]\d+)?)\s*[₽]',
    r'"price"\s*:\s*"([\d\s]+)\s*₽"',
    r'finalPrice":"([\d\s]+)\s*₽'
]

CONDITIONS: List[dict] = [
    {"min_offset": -100, "max_offset": -40, "old_price_multiplier": 1.20, "price_multiplier": 1.20, "min_price_discount": 0.10},
    {"min_offset": -40, "max_offset": -30, "old_price_multiplier": 1.18, "price_multiplier": 1.18, "min_price_discount": 0.10},
    {"min_offset": -30, "max_offset": -20, "old_price_multiplier": 1.16, "price_multiplier": 1.16, "min_price_discount": 0.10},
    {"min_offset": -20, "max_offset": -10, "old_price_multiplier": 1.14, "price_multiplier": 1.14, "min_price_discount": 0.10},
    {"min_offset": -10, "max_offset": 3, "old_price_multiplier": 1.12, "price_multiplier": 1.12, "min_price_discount": 0.10},
    {"min_offset": 3, "max_offset": 15, "old_price_multiplier": 0.93, "price_multiplier": 0.93, "min_price_discount": 0.03},
]
