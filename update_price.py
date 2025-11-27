# update_price.py


import time
import re
import os
import glob
import shutil
import math
import random
import requests
from loguru import logger
from typing import List, Dict, Optional, Tuple
import threading
from threading import Lock
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import zipfile
from urllib.parse import urlparse
from fake_useragent import UserAgent
import undetected_chromedriver as uc
from requests.auth import HTTPProxyAuth
import conf as Config


class TrafficMonitor:
    """Мониторинг сетевого трафика"""
    def __init__(self):
        self.lock = Lock()
        self.total_bytes_received = 0
        self.total_bytes_sent = 0
        self.url_traffic = {}
    
    def add_traffic(self, url, bytes_received, bytes_sent):
        with self.lock:
            self.total_bytes_received += bytes_received
            self.total_bytes_sent += bytes_sent
            if url in self.url_traffic:
                self.url_traffic[url]["received"] += bytes_received
                self.url_traffic[url]["sent"] += bytes_sent
            else:
                self.url_traffic[url] = {"received": bytes_received, "sent": bytes_sent}
            self._log_traffic(url)
            
    def _log_traffic(self, url):
        received = self.url_traffic[url]["received"]
        sent = self.url_traffic[url]["sent"]
        total = received + sent
        def format_bytes(count):
            if count < 1024: return f"{count} B"
            elif count < 1024**2: return f"{count/1024:.2f} KB"
            else: return f"{count/(1024**2):.2f} MB"
        logger.info(f"[Traffic] {url}: received {format_bytes(received)}, sent {format_bytes(sent)}, total {format_bytes(total)}")

    def get_total_traffic(self):
        with self.lock:
            total = self.total_bytes_received + self.total_bytes_sent
            if total < 1024: return f"{total} B"
            elif total < 1024**2: return f"{total/1024:.2f} KB"
            else: return f"{total/(1024**2):.2f} MB"

# Global traffic monitor instance
traffic_monitor = TrafficMonitor()

class ProxyManager:
    """Управление прокси"""
    def __init__(self, proxies_file="proxies.txt"):
        self.lock = threading.Lock()
        self.proxies: List[Tuple[str, Optional[Dict[str, str]]]] = []
        self.index = 0
        self.ua = UserAgent(platforms=['desktop'], browsers=['chrome', 'firefox', 'edge'])
        self._load_proxies(proxies_file)

    def _load_proxies(self, path):
        if not os.path.isfile(path):
            logger.warning(f"{path} not found, using direct connection.")
            self.proxies = [("direct", None)]
            return

        with open(path, 'r') as f:
            lines = [ln.strip() for ln in f if ln.strip()]

        for raw in lines:
            try:
                proxy_str = raw
                if '://' not in proxy_str: proxy_str = 'http://' + proxy_str
                parsed = urlparse(proxy_str)
                if parsed.scheme not in Config.SUPPORTED_SCHEMES:
                    logger.warning(f"Skip unsupported scheme {parsed.scheme} in {proxy_str}")
                    continue
                host = parsed.hostname
                port = parsed.port
                if not host or not port:
                    logger.warning(f"Invalid proxy address: {proxy_str}")
                    continue
                cred = None
                if parsed.username and parsed.password:
                    cred = {'username': parsed.username, 'password': parsed.password}
                server = f"{parsed.scheme}://{host}:{port}"
                if self._check_proxy_simple(server, cred):
                    self.proxies.append((server, cred))
                    logger.info(f"Added proxy: {server}")
                if len(self.proxies) >= Config.MAX_PROXIES: break
            except Exception as e:
                logger.warning(f"Error parsing proxy '{raw}': {e}")

        if not self.proxies:
            self.proxies = [("direct", None)]
            logger.warning("No valid proxies found, using direct connection.")
        else:
            logger.info(f"Loaded proxies: {[p[0] for p in self.proxies]}")

    def _check_proxy_simple(self, server, credentials):
        proxies = {"http": server, "https": server}
        auth = None
        if credentials: auth = HTTPProxyAuth(credentials['username'], credentials['password'])
        try:
            resp = requests.get(Config.HTTPBIN_URL, proxies=proxies, auth=auth, timeout=Config.TIMEOUT)
            traffic_monitor.add_traffic('proxy_check', len(resp.content), 0)
            return resp.ok
        except Exception: return False

    def get_proxy(self):
        with self.lock:
            if not self.proxies: return ("direct", None)
            proxy = self.proxies[self.index]
            self.index = (self.index + 1) % len(self.proxies)
            logger.debug(f"Using proxy: {proxy[0]}")
            return proxy

    def get_random_user_agent(self):
        try: return self.ua.random if self.ua else random.choice(Config.STATIC_USER_AGENTS)
        except: return random.choice(Config.STATIC_USER_AGENTS)
        
class Parser:
    """Парсер страниц Ozon"""
    def __init__(self, proxy_manager: ProxyManager, traffic_monitor: TrafficMonitor):
        self.proxy_manager = proxy_manager
        self.traffic_monitor = traffic_monitor
        self.proxy_info = proxy_manager.get_proxy()
        self.user_agent = proxy_manager.get_random_user_agent()
        self.driver = self.setup_driver()
        self.anti_bot_counter = 0
        self.warm_up()
        
    def setup_driver(self):
        options = uc.ChromeOptions()
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--lang=ru-RU,ru")
        options.add_argument(f"--user-agent={self.user_agent}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-features=IsolateOrigins,site-per-process")
        #options.add_argument("--headless=new")

        proxy_server, credentials = self.proxy_info
        if proxy_server != "direct":
            parsed_proxy = urlparse(proxy_server)
            host, port = parsed_proxy.hostname, parsed_proxy.port
            scheme = parsed_proxy.scheme
            if credentials:
                manifest_json = """
                {
                    "version": "1.0.0",
                    "manifest_version": 2,
                    "name": "Proxy",
                    "permissions": [
                        "proxy",
                        "tabs",
                        "unlimitedStorage",
                        "storage",
                        "<all_urls>",
                        "webRequest",
                        "webRequestBlocking"
                    ],
                    "background": {
                        "scripts": ["background.js"]
                    },
                    "minimum_chrome_version":"22.0.0"
                }
                """
                background_js = f"""
                var config = {{
                    mode: "fixed_servers",
                    rules: {{
                        singleProxy: {{
                            scheme: "{scheme}",
                            host: "{host}",
                            port: parseInt({port})
                        }},
                        bypassList: ["localhost"]
                    }}
                }};

                chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});

                function callbackFn(details) {{
                    return {{
                        authCredentials: {{
                            username: "{credentials['username']}",
                            password: "{credentials['password']}"
                        }}
                    }};
                }}

                chrome.webRequest.onAuthRequired.addListener(
                    callbackFn,
                    {{urls: ["<all_urls>"]}},
                    ['blocking']
                );
                """
                plugin_file = 'proxy_auth_plugin.zip'
                with zipfile.ZipFile(plugin_file, 'w') as zp:
                    zp.writestr("manifest.json", manifest_json)
                    zp.writestr("background.js", background_js)
                options.add_extension(plugin_file)
            else:
                options.add_argument(f'--proxy-server={proxy_server}')

        try:
            driver = uc.Chrome(
                options=options,
                driver_executable_path=ChromeDriverManager().install(),
                version_main=139,
                headless=False
            )
            driver.set_page_load_timeout(Config.TIMEOUT)
            driver.set_script_timeout(Config.TIMEOUT)
            stealth_js = """
            // Удаляем нативные функции WebDriver
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            // Переопределяем свойство plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            // Переопределяем свойство languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['ru-RU', 'ru', 'en']
            });
            // Добавляем Chrome объект
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };
            
            // Переопределяем permissions
            const originalQuery = window.navigator.permissions.query;
            return window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            """
            
            driver.execute_script(stealth_js)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => false
                });
                """
            })
            return driver
        except Exception as e:
            logger.error(f"Driver setup error: {e}")
            if os.path.exists("proxy_auth_plugin.zip"): os.remove("proxy_auth_plugin.zip")
            raise

    def warm_up(self):
        try:
            if not self.driver: return
            sites = ["https://ya.ru", "https://wikipedia.org"]
            for site in sites:
                try:
                    self.driver.get(site)
                    time.sleep(random.uniform(1, 3))
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/4);")
                    time.sleep(random.uniform(0.5, 1.5))
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                    time.sleep(random.uniform(0.5, 1))
                except Exception as e: logger.warning(f"Warm-up error on {site}: {e}")
            logger.info("Browser warmed up")
        except Exception as e: logger.warning(f"Browser warm-up error: {e}")

    def quit(self):
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
                logger.info("Driver closed")
        except Exception as e: logger.warning(f"Driver quit error: {e}")

    def simulate_human_behavior(self):
        if not self.driver: return
        try:
            viewport_height = self.driver.execute_script("return window.innerHeight")
            page_height = self.driver.execute_script("return document.body.scrollHeight")
            for i in range(1, 5):
                scroll_to = min(i * viewport_height / 3, page_height - viewport_height)
                self.driver.execute_script(f"window.scrollTo(0, {scroll_to});")
                time.sleep(random.uniform(0.5, 1.5))
            self.driver.execute_script("""
                // Имитация движения мыши
                const simulateMouseMove = (x, y) => {
                    const event = new MouseEvent('mousemove', {
                        'view': window,
                        'clientX': x,
                        'clientY': y,
                        'bubbles': true,
                        'cancelable': true
                    });
                    document.elementFromPoint(x, y).dispatchEvent(event);
                };
                
                // Случайные движения мыши
                const steps = 10;
                const startX = Math.floor(Math.random() * window.innerWidth);
                const startY = Math.floor(Math.random() * window.innerHeight);
                
                for (let i = 0; i < steps; i++) {
                    const x = startX + Math.random() * 50 - 25;
                    const y = startY + Math.random() * 50 - 25;
                    simulateMouseMove(x, y);
                }
            """)
            time.sleep(random.uniform(1, 2))
        except Exception as e: logger.warning(f"Human behavior simulation error: {e}")

    def parse_price(self, url: str) -> str | None:
        if not self.driver:
            try:
                self.driver = self.setup_driver()
                self.warm_up()
            except Exception as e:
                logger.error(f"Driver init failed: {e}")
                return None

        retries = 0
        while retries < Config.MAX_RETRIES:
            try:
                logger.info(f"Loading URL: {url}")
                self.driver.get(url)
                time.sleep(random.uniform(*Config.REQUEST_DELAY))
                if self.driver:
                    page_source = self.driver.page_source
                    bytes_rcv = len(page_source.encode('utf-8'))
                    traffic_monitor.add_traffic(url, bytes_rcv, 0)
                    self.simulate_human_behavior()
                    if self.is_blocked():
                        self.handle_block()
                        retries += 1
                        continue
                    price = self.extract_price()
                    if price: return price
                retries += 1
                self.rotate_identity()
            except Exception as e: logger.error(f"Parsing error {url}: {e}")
        logger.error(f"Price extraction failed: {url}")
        return None

    def extract_price(self) -> str | None:
        if not self.driver:
            return None

        # 1. Новый метод: поиск через data-widget и кнопку
        try:
            web_price_block = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-widget='webPrice']"))
            )
            # Ищем все span внутри кнопки
            buttons = web_price_block.find_elements(By.CSS_SELECTOR, "button")
            for button in buttons:
                spans = button.find_elements(By.TAG_NAME, "span")
                for span in spans:
                    raw_price = span.text.strip()
                    if '₽' in raw_price:
                        # Очистка через регулярное выражение
                        clean_price = re.sub(r'[^\d]', '', raw_price)
                        if clean_price.isdigit():
                            return clean_price
        except Exception as e:
            logger.debug(f"Data-widget parsing failed: {e}")

        # 2. Резерв: старые селекторы
        for selector in Config.PRICE_SELECTORS[1:]:
            try:
                price_element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                raw_price = price_element.text.strip()
                if '₽' in raw_price:
                    clean_price = re.sub(r'[^\d]', '', raw_price)
                    if clean_price.isdigit():
                        return clean_price
            except:
                continue

        # 3. Финальный резерв: регулярные выражения
        page_source = self.driver.page_source
        for pattern in Config.PRICE_PATTERNS:
            matches = re.findall(pattern, page_source)
            if matches:
                clean_price = re.sub(r'[^\d]', '', matches[0])
                if clean_price.isdigit():
                    return clean_price

        return None

    def is_blocked(self) -> bool:
        if not self.driver: return False
        blocks = [
            "//*[contains(text(), 'Доступ ограничен')]",
            "//*[contains(text(), 'Подозрительная активность')]",
            "//*[contains(text(), 'Проверка безопасности')]",
            "//*[contains(text(), 'Cloudflare')]",
            "//*[contains(text(), 'Please verify you are a human')]"
        ]
        for xpath in blocks:
            try:
                element = self.driver.find_element(By.XPATH, xpath)
                if element.is_displayed(): return True
            except: continue
        try: 
            return self.driver.execute_script("""
                return document.body.innerHTML.includes('Доступ ограничен') || 
                       document.body.innerHTML.includes('Подозрительная активность') ||
                       document.title.includes('Security check') ||
                       document.querySelector('iframe[src*=\"challenge\"]') !== null;
            """)
        except: return False

    def handle_block(self):
        if not self.driver: return
        try:
            self.anti_bot_counter += 1
            # Button click attempt
            update_buttons = [
                "//button[contains(text(), 'Обновить')]",
                "//button[contains(text(), 'Продолжить')]",
                "//button[contains(text(), 'Verify')]"
            ]
            for xpath in update_buttons:
                try:
                    button = WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.XPATH, xpath)))
                    button.click()
                    time.sleep(5)
                    return
                except: continue
            # Identity rotation
            if self.anti_bot_counter >= 2:
                self.rotate_identity()
                time.sleep(Config.PROXY_CHANGE_DELAY)
                return
            # JavaScript bypass
            try:
                self.driver.execute_script("""
                    // Попытка скрыть блокирующий элемент
                    const blockers = document.querySelectorAll('div[style*=\"blur\"], div[class*=\"overlay\"]');
                    blockers.forEach(el => {
                        el.style.display = 'none';
                    });
                    // Показываем основной контент
                    const mainContent = document.querySelector('body > div:not([style*=\"blur\"])');
                    if (mainContent) mainContent.style.display = 'block';
                """)
                time.sleep(5)
                return
            except: pass
            # Full reload
            self.rotate_identity()
        except Exception as e: logger.error(f"Bypass error: {e}")

    def rotate_identity(self):
        try:
            current_url = self.driver.current_url if self.driver else None
            self.quit()
            self.proxy_info = self.proxy_manager.get_proxy()
            self.user_agent = self.proxy_manager.get_random_user_agent()
            time.sleep(Config.PROXY_CHANGE_DELAY)
            self.driver = self.setup_driver()
            self.warm_up()
            if current_url: self.driver.get(current_url)
            self.anti_bot_counter = 0
        except Exception as e: logger.error(f"Identity rotation error: {e}")

# ========== Функции для обновления цен на Ozon ==========
def round_price(price):
    """Округляет цену до целого числа"""
    return int(round(price))

def calculate_deviation(price_1c: float, ozon_price: float) -> float:
    """Расчет отклонения цены в процентах"""
    if price_1c == 0:
        return 0.0
    return ((ozon_price - price_1c) / price_1c) * 100

def calculate_prices_for_api(base_price: float, condition: dict) -> tuple[float, float, float]:
    """Вычисление цен для API с учетом требований Ozon"""
    # Рассчитываем old_price и базовую цену
    old_price = base_price * condition['old_price_multiplier']
    candidate_price = base_price * condition['price_multiplier']
    
    # Применяем правило скидки >5% для диапазона 400-10000
    if 400 <= candidate_price <= 10000:
        # Цена должна быть меньше 95% от old_price минус 1 рубль
        max_allowed_price = math.floor(old_price * 0.95) - 1
        price = min(candidate_price, max_allowed_price)
    else:
        price = candidate_price
    
    # КРИТИЧЕСКИ ВАЖНО: min_price всегда должен быть меньше price
    min_price = price * (1 - condition['min_price_discount'])
    
    # Дополнительная проверка на корректность
    if min_price >= price:
        min_price = price * 0.9
        logger.warning(f"Adjusted min_price to be less than price: {min_price:.2f} < {price:.2f}")
    
    # Проверяем что old_price больше price (для корректного отображения скидки)
    if old_price <= price:
        old_price = price * 1.05
        logger.warning(f"Adjusted old_price to be greater than price: {old_price:.2f} > {price:.2f}")
    
    return old_price, price, min_price

def update_ozon_prices(offer_id: str, old_price: float, price: float, min_price: float) -> bool:
    """Обновление цен товара через API Ozon"""
    # Округляем все цены до целых чисел
    old_price_int = round_price(old_price) if old_price > 0 else 0
    price_int = round_price(price)
    min_price_int = round_price(min_price)
    
    # Проверки перед отправкой
    if min_price_int >= price_int:
        min_price_int = max(1, price_int - 1)
        logger.warning(f"Adjusted min_price for {offer_id} to {min_price_int}")
    
    if old_price_int > 0 and old_price_int <= price_int:
        old_price_int = price_int + 1
        logger.warning(f"Adjusted old_price for {offer_id} to {old_price_int}")
    
    # Формируем запрос
    url = f"{Config.BASE_URL}/v1/product/import/prices"
    headers = {
        "Client-Id": Config.CLIENT_ID,
        "Api-Key": Config.API_KEY,
        "Content-Type": "application/json"
    }
    
    payload = {"prices": [{
        "offer_id": str(offer_id),
        "old_price": str(old_price_int),
        "price": str(price_int),
        "min_price": str(min_price_int),
        "currency_code": "RUB",
        "min_price_for_auto_actions_enabled": True,
        "price_strategy_enabled": "DISABLED"
    }]}

    for attempt in range(1, Config.MAX_API_ATTEMPTS + 1):
        logger.info(f"Updating {offer_id} (attempt {attempt}): old={old_price_int}, price={price_int}, min={min_price_int}")
        
        try:
            response = requests.post(
                url, 
                json=payload, 
                headers=headers, 
                timeout=Config.API_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                # Проверяем результат обновления
                for item in data.get("result", []):
                    if item.get("offer_id") == offer_id and item.get("updated"):
                        logger.success(f"Price updated successfully for {offer_id}")
                        return True
                # Логируем ошибки валидации
                for item in data.get("result", []):
                    if item.get("offer_id") == offer_id:
                        errors = item.get("errors", [])
                        for error in errors:
                            logger.error(f"Validation error for {offer_id}: {error}")
                logger.error(f"Failed to update prices for {offer_id}: {data}")
                return False
                
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', Config.BACKOFF_BASE ** attempt))
                retry_after = min(retry_after, Config.BACKOFF_MAX)
                logger.warning(f"Rate limit for {offer_id}, sleeping {retry_after}s")
                time.sleep(retry_after)
                continue
                
            else:
                logger.error(f"HTTP {response.status_code} for {offer_id}: {response.text}")
                time.sleep(Config.BACKOFF_BASE ** attempt)
                
        except Exception as e:
            logger.error(f"Exception for {offer_id} on attempt {attempt}: {e}")
            time.sleep(Config.BACKOFF_BASE ** attempt)
    
    logger.error(f"Max attempts reached for update_prices {offer_id}")
    return False

def get_condition(offset: float) -> dict:
    """Выбор условия обработки по проценту отклонения"""
    for cond in Config.CONDITIONS:
        if cond["min_offset"] <= offset <= cond["max_offset"]:
            return cond
    return Config.CONDITIONS[-1]  # Условие по умолчанию

def parse_price_str(price_str: str) -> float:
    """Преобразование строки цены в число"""
    clean = re.sub(r'[^\d]', '', price_str)
    try:
        return float(clean) if clean else 0.0
    except ValueError:
        return 0.0

def find_latest_bad_price_file() -> Optional[str]:
    """Поиск последнего файла с проблемными ценами"""
    try:
        files = glob.glob("in/bad_price_*.txt")
        if not files:
            return None
        # Сортировка по дате создания (по имени файла)
        files.sort(key=os.path.getmtime, reverse=True)
        return files[0]
    except Exception as e:
        logger.error(f"Ошибка поиска файлов: {str(e)}")
        return None

def prepare_in_work_file(source_file: str) -> Optional[str]:
    """Подготовка рабочего файла"""
    try:
        os.makedirs("in_work", exist_ok=True)
        dest_file = os.path.join("in_work", "inwork.txt")
        shutil.copy(source_file, dest_file)
        logger.info(f"Создан рабочий файл: {dest_file}")
        return dest_file
    except Exception as e:
        logger.error(f"Ошибка подготовки файла: {str(e)}")
        return None

def process_product_line(line: str, parser: Parser) -> str:
    """Обработка строки с товаром"""
    parts = line.strip().split()
    if len(parts) < 11:
        return line
    
    try:
        # Извлечение данных из строки
        product_id = parts[0]
        sku = parts[1]
        offer_id = parts[2]
        offset_str = parts[3].rstrip('%')
        base_price = parts[4]
        old_price = parts[5]
        min_price = parts[6]
        price_1c = parts[7]
        card_price = parts[8]
        product_name = " ".join(parts[9:-1])
        url = parts[-1]
        
        # Преобразование числовых значений
        price_1c_val = float(price_1c)
        
        # Основной цикл обработки товара
        for attempt in range(1, Config.MAX_ATTEMPTS_PER_PRODUCT + 1):
            # 1. Парсим текущую цену "С Ozon картой"
            logger.info(f"Парсинг цены: {url}")
            ozon_price_str = parser.parse_price(url)
            
            if not ozon_price_str:
                logger.warning("Цена не получена, попытка пропущена")
                time.sleep(20)
                continue
                
            ozon_price = parse_price_str(ozon_price_str)
            parts[8] = str(ozon_price)
            
            # 2. Вычисляем текущее отклонение
            current_offset = calculate_deviation(price_1c_val, ozon_price)
            formatted_offset = f"{current_offset:.2f}%"
            parts[3] = formatted_offset
            logger.info(f"Текущее отклонение: {formatted_offset}")
            
            
            
            
            """
            ВМЕСТО ЭТОГО ДИАПАЗОНА НУЖНО РАСЧИТАТЬ ДРУГОЙ
            
            # 3. Проверяем, находится ли цена в допустимом диапазоне (±3%)
            lower_bound = price_1c_val * (1 - Config.PRICE_TOLERANCE)
            upper_bound = price_1c_val * (1 + Config.PRICE_TOLERANCE)
            logger.info(f"Диапазон цен: {lower_bound:.2f}-{upper_bound:.2f}, текущая: {ozon_price}")
            
            """
            # 3. Проверяем, находится ли цена в допустимом диапазоне (±3%)
            lower_bound = price_1c_val
            upper_bound = price_1c_val * (1 + Config.PRICE_TOLERANCE)
            logger.info(f"Диапазон цен: {lower_bound:.2f}-{upper_bound:.2f}, текущая: {ozon_price}")
            
            if lower_bound <= ozon_price <= upper_bound:
                logger.success(f"Цена в диапазоне: {ozon_price}")
                break
            else:
                logger.warning(f"Цена вне диапазона: {ozon_price} не входит в [{lower_bound:.2f}, {upper_bound:.2f}]")
                
                # 4. Определяем условие обработки по текущему отклонению
                condition = get_condition(current_offset)
                logger.info(f"Условие для отклонения {current_offset}%: {condition}")
                
                # 5. Рассчитываем новые цены
                try:
                    base_val = float(base_price)
                    new_old, new_price, new_min = calculate_prices_for_api(base_val, condition)
                    
                    # Округление цен
                    new_old = round(new_old)
                    new_price = round(new_price)
                    new_min = round(new_min)
                    
                    # Обновление данных в строке
                    parts[4] = str(new_price)
                    parts[5] = str(new_old)
                    parts[6] = str(new_min)
                    
                    # 6. Обновляем цены через API
                    logger.info(f"Отправка обновленных цен для {offer_id}")
                    if update_ozon_prices(offer_id, new_old, new_price, new_min):
                        logger.success(f"Цены успешно обновлены на Ozon для {offer_id}")
                    else:
                        logger.error(f"Ошибка при обновлении цен на Ozon для {offer_id}")
                        
                    # 7. Обновляем базовую цену для возможной следующей итерации
                    base_price = str(new_price)
                except Exception as e:
                    logger.error(f"Ошибка расчета цен: {str(e)}")
                    break
                
                # Задержка перед повторной проверкой
                retry_delay = random.uniform(2, 5)  # Увеличена задержка для обновления цен на Ozon
                logger.info(f"Ожидание обновления цен {retry_delay:.1f} сек. (попытка {attempt})")
                time.sleep(retry_delay)
        else:
            logger.warning(f"Достигнуто максимальное количество попыток для товара")
        
        # Формирование обновленной строки
        return " ".join(parts)
        
    except Exception as e:
        logger.error(f"Критическая ошибка обработки: {str(e)}")
        return line

def process_in_work_file(in_work_file: str, proxy_manager: ProxyManager):
    """Обработка рабочего файла"""
    traffic_monitor = TrafficMonitor()
    parser = Parser(proxy_manager, traffic_monitor)
    
    with open(in_work_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    total_lines = len(lines)
    logger.info(f"Начата обработка {total_lines} товаров")
    
    # Обработка каждой строки с немедленным сохранением
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
            
        logger.info(f"Обработка товара {i+1}/{total_lines}")
        processed_line = process_product_line(line, parser)
        lines[i] = processed_line + "\n"
        
        # Немедленное сохранение прогресса
        with open(in_work_file, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        # Задержка между товарами
        if i < total_lines - 1:
            delay = random.uniform(*Config.PRODUCT_DELAY_RANGE)
            logger.info(f"Пауза {delay:.1f} сек.")
            time.sleep(delay)
    
    logger.success(f"Файл обработан: {in_work_file}")
    parser.quit()

def move_processed_file(source_file: str):
    """Перемещение обработанного файла"""
    try:
        processed_dir = "in/processed"
        os.makedirs(processed_dir, exist_ok=True)
        filename = os.path.basename(source_file)
        dest_path = os.path.join(processed_dir, filename)
        shutil.move(source_file, dest_path)
        logger.info(f"Файл перемещен: {dest_path}")
    except Exception as e:
        logger.error(f"Ошибка перемещения файла: {str(e)}")

def check_file_age(file_path: str) -> bool:
    """Проверка возраста файла (в минутах)"""
    if not os.path.exists(file_path):
        return True
        
    file_time = os.path.getmtime(file_path)
    current_time = time.time()
    age_minutes = (current_time - file_time) / 60
    return age_minutes > Config.MAX_FILE_AGE_MINUTES

def main():
    """Основной цикл программы"""
    logger.info("Запуск Ozon Price Corrector")
    proxy_manager = ProxyManager()
    
    # Создаем необходимые директории
    os.makedirs("in", exist_ok=True)
    os.makedirs("in/processed", exist_ok=True)
    os.makedirs("in_work", exist_ok=True)
    
    while True:
        try:
            in_work_dir = "in_work"
            in_work_file_path = os.path.join(in_work_dir, "inwork.txt")
            latest_bad = find_latest_bad_price_file()
            
            # Переменная для отслеживания, нужно ли обрабатывать файл
            should_process = False
            work_file_to_process = None
            
            if os.path.exists(in_work_file_path):
                logger.info(f"Найден рабочий файл: {in_work_file_path}")
                
                # Проверяем возраст файла
                if check_file_age(in_work_file_path):
                    logger.info("Файл устарел (более 30 минут)")
                    
                    if latest_bad:
                        bad_mtime = os.path.getmtime(latest_bad)
                        work_mtime = os.path.getmtime(in_work_file_path)
                        
                        if bad_mtime > work_mtime:
                            logger.info("Найден новый файл bad_price, обновляем рабочий файл")
                            new_work_file = prepare_in_work_file(latest_bad)
                            if new_work_file and os.path.exists(new_work_file):
                                work_file_to_process = new_work_file
                                should_process = True
                            else:
                                logger.error("Не удалось создать новый рабочий файл")
                        else:
                            logger.info("Новых файлов не найдено, продолжаем обработку")
                            work_file_to_process = in_work_file_path
                            should_process = True
                    else:
                        logger.info("Файлов bad_price не найдено, продолжаем обработку")
                        work_file_to_process = in_work_file_path
                        should_process = True
                else:
                    logger.info("Файл актуален, продолжаем обработку")
                    work_file_to_process = in_work_file_path
                    should_process = True
            else:
                logger.info("Рабочий файл не найден")
                if latest_bad:
                    logger.info(f"Найден файл для обработки: {latest_bad}")
                    new_work_file = prepare_in_work_file(latest_bad)
                    if new_work_file and os.path.exists(new_work_file):
                        work_file_to_process = new_work_file
                        should_process = True
                    else:
                        logger.error("Не удалось создать рабочий файл")
                else:
                    logger.info("Файлы для обработки не найдены")
            
            # Если нужно обработать файл и у нас есть валидный путь
            if should_process and work_file_to_process:
                process_in_work_file(work_file_to_process, proxy_manager)
                
                # После обработки перемещаем исходный bad_price файл
                if latest_bad and os.path.exists(latest_bad):
                    move_processed_file(latest_bad)
            
            # Пауза перед следующей проверкой
            logger.info(f"Ожидание следующей проверки через {Config.FILE_CHECK_INTERVAL} сек.")
            time.sleep(Config.FILE_CHECK_INTERVAL)
            
        except KeyboardInterrupt:
            logger.info("Работа завершена по запросу пользователя")
            break
        except Exception as e:
            logger.error(f"Критическая ошибка: {str(e)}")
            time.sleep(60)

if __name__ == "__main__":
    # Настройка логгера
    logger.add(
        "logs/update_price.log",
        rotation="10 MB",
        retention="7 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )
    
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        logger.info("Program terminated")