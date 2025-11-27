import pandas as pd
import random
import time
import re
from queue import Queue
from threading import Thread, Lock
from loguru import logger
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import requests
from requests.auth import HTTPProxyAuth
import zipfile
import threading
import os
import sys
from typing import List, Tuple, Dict, Optional
from urllib.parse import urlparse
from fake_useragent import UserAgent
import undetected_chromedriver as uc

# Traffic monitoring class


class TrafficMonitor:
    """Класс для мониторинга сетевого трафика"""

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
                self.url_traffic[url] = {
                    "received": bytes_received, "sent": bytes_sent}
            self._log_traffic(url)

    def _log_traffic(self, url):
        received = self.url_traffic[url]["received"]
        sent = self.url_traffic[url]["sent"]
        total = received + sent

        def format_bytes(count):
            if count < 1024:
                return f"{count} B"
            elif count < 1024**2:
                return f"{count/1024:.2f} KB"
            else:
                return f"{count/(1024**2):.2f} MB"
        logger.info(
            f"[Traffic] {url}: received {format_bytes(received)}, sent {format_bytes(sent)}, total {format_bytes(total)}")

    def get_total_traffic(self):
        with self.lock:
            total = self.total_bytes_received + self.total_bytes_sent
            if total < 1024:
                return f"{total} B"
            elif total < 1024**2:
                return f"{total/1024:.2f} KB"
            else:
                return f"{total/(1024**2):.2f} MB"

    def get_stats(self):
        with self.lock:
            return {
                "total_received": self.total_bytes_received,
                "total_sent": self.total_bytes_sent,
                "total": self.total_bytes_received + self.total_bytes_sent,
                "url_details": self.url_traffic
            }


# Global traffic monitor instance
traffic_monitor = TrafficMonitor()


class Config:
    THREADS_PER_PROXY = 1  # Уменьшено для снижения нагрузки на прокси
    MAX_PROXIES = 1  # Увеличено для лучшей ротации
    REQUEST_DELAY = (1, 3)  # Увеличенные паузы между запросами
    PROXY_CHANGE_DELAY = 1  # Увеличенное время ожидания после смены прокси
    MAX_RETRIES = 2  # Больше попыток для надежности
    TIMEOUT = 2  # Таймаут для запросов
    # URL для проверки работоспособности прокси
    HTTPBIN_URL = "https://httpbin.org/ip"
    # Поддерживаемые схемы прокси
    SUPPORTED_SCHEMES = ("http", "https")

    # Расширенный список User-Agent
    STATIC_USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.57"
    ]

    # Известные селекторы для цен на Ozon
    PRICE_SELECTORS = [

        "div[data-widget='webPrice'] button span"

    ]
    

    PRICE_PATTERNS = [
        r'(\d+[\s.]?\d+)\s*[₽]',
        r'[\D](\d{1,3}(?:\s?\d{3})*(?:[.,]\d+)?)\s*[₽]',
        r'"price"\s*:\s*"([\d\s]+)\s*₽"',
        r'finalPrice":"([\d\s]+)\s*₽'
    ]


class ProxyManager:
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
                # Добавляем схему по умолчанию, если отсутствует
                if '://' not in proxy_str:
                    proxy_str = 'http://' + proxy_str
                parsed = urlparse(proxy_str)

                # Фильтрация неподдерживаемых схем
                if parsed.scheme not in Config.SUPPORTED_SCHEMES:
                    logger.warning(
                        f"Skip unsupported scheme {parsed.scheme} in {proxy_str}")
                    continue

                host = parsed.hostname
                port = parsed.port
                if not host or not port:
                    logger.warning(f"Invalid proxy address: {proxy_str}")
                    continue

                # Учётные данные при наличии
                cred = None
                if parsed.username and parsed.password:
                    cred = {
                        'username': parsed.username,
                        'password': parsed.password
                    }

                server = f"{parsed.scheme}://{host}:{port}"

                # Быстрая проверка прокси
                if self._check_proxy_simple(server, cred):
                    self.proxies.append((server, cred))
                    logger.info(f"Added proxy: {server}")

                # Ограничиваем пул прокси
                if len(self.proxies) >= Config.MAX_PROXIES:
                    break

            except Exception as e:
                logger.warning(f"Error parsing proxy '{raw}': {e}")

        if not self.proxies:
            # Всегда есть опция прямого соединения
            self.proxies = [("direct", None)]
            logger.warning("No valid proxies found, using direct connection.")
        else:
            logger.info(f"Loaded proxies: {[p[0] for p in self.proxies]}")

    def _check_proxy_simple(self, server, credentials):
        proxies = {"http": server, "https": server}
        auth = None
        if credentials:
            auth = HTTPProxyAuth(
                credentials['username'], credentials['password'])
        try:
            resp = requests.get(
                Config.HTTPBIN_URL, proxies=proxies, auth=auth, timeout=Config.TIMEOUT)
            # log traffic from proxy check
            traffic_monitor.add_traffic('proxy_check', len(resp.content), 0)
            return resp.ok
        except Exception:
            return False

    def get_proxy(self):
        with self.lock:
            if not self.proxies:
                return ("direct", None)
            proxy = self.proxies[self.index]
            self.index = (self.index + 1) % len(self.proxies)
            logger.debug(f"Using proxy: {proxy[0]}")
            return proxy

    def get_random_user_agent(self):
        try:
            return self.ua.random if self.ua else random.choice(Config.STATIC_USER_AGENTS)
        except:
            return random.choice(Config.STATIC_USER_AGENTS)


class Parser:
    def __init__(self, proxy_manager: ProxyManager):
        self.proxy_manager = proxy_manager
        # Теперь это кортеж (server, credentials)
        self.proxy_info = self.proxy_manager.get_proxy()
        self.user_agent = self.proxy_manager.get_random_user_agent()
        self.driver = None
        self.anti_bot_counter = 0  # Счетчик встреч с анти-ботом

        # Попытка инициализации драйвера
        try:
            self.driver = self.setup_driver()
            # Предварительный прогрев браузера
            self.warm_up()
        except Exception as e:
            logger.error(f"Ошибка инициализации драйвера: {e}")
            if self.driver:
                self.driver.quit()
            self.driver = None
            raise

    def setup_driver(self):
        """Настройка и запуск Selenium с undetected_chromedriver для обхода защиты"""
        # Используем undetected_chromedriver для обхода обнаружения автоматизации
        options = uc.ChromeOptions()

        # Общие настройки для всех соединений
        
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--lang=ru-RU,ru")
        options.add_argument(f"--user-agent={self.user_agent}")
        options.add_argument("--disable-blink-features=AutomationControlled")

        # Добавление специальных заголовков для маскировки
        options.add_argument(
            "--disable-features=IsolateOrigins,site-per-process")

        # Настройки прокси
        proxy_server, credentials = self.proxy_info

        if proxy_server != "direct":
            parsed_proxy = urlparse(proxy_server)
            host, port = parsed_proxy.hostname, parsed_proxy.port
            scheme = parsed_proxy.scheme

            # Если есть учетные данные, создаем плагин расширения для аутентификации
            if credentials:
                manifest_json = """
                {
                    "version": "1.0.0",
                    "manifest_version": 2,
                    "name": "Chrome Proxy",
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

                # Создаем ZIP с расширением
                with zipfile.ZipFile(plugin_file, 'w') as zp:
                    zp.writestr("manifest.json", manifest_json)
                    zp.writestr("background.js", background_js)

                # Добавляем расширение в ChromeOptions
                options.add_extension(plugin_file)

            else:
                # Для прокси без аутентификации используем стандартный способ
                options.add_argument(f'--proxy-server={proxy_server}')

        # Создаем undetected_chromedriver
        try:
            driver = uc.Chrome(
                options=options,
                driver_executable_path=ChromeDriverManager().install(),
                headless=False  # Для обхода обнаружения лучше использовать видимый режим
            )

            # Устанавливаем таймаут загрузки страницы и скриптов
            driver.set_page_load_timeout(Config.TIMEOUT)
            driver.set_script_timeout(Config.TIMEOUT)

            # Добавляем дополнительные скрипты для обхода обнаружения автоматизации
            stealth_js = """
            // Переопределение свойств navigator
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false
            });
            
            // Переопределение permissions API
            if (navigator.permissions) {
                navigator.permissions.query = (function (original) {
                    return function (parameters) {
                        if (parameters.name === 'notifications') {
                            return Promise.resolve({state: Notification.permission});
                        }
                        return original.apply(this, arguments);
                    };
                })(navigator.permissions.query);
            }
            
            // Имитация поведения пользователя при прокрутке
            (function(){
                const newProto = navigator.__proto__;
                delete newProto.webdriver;
                navigator.__proto__ = newProto;
            })();
            
            // Имитация плагинов, как у обычного браузера
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    {
                        0: {type: "application/x-google-chrome-pdf"},
                        description: "Portable Document Format",
                        filename: "internal-pdf-viewer",
                        length: 1,
                        name: "Chrome PDF Plugin"
                    },
                    {
                        0: {type: "application/pdf"},
                        description: "Portable Document Format",
                        filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
                        length: 1,
                        name: "Chrome PDF Viewer"
                    },
                    {
                        0: {type: "application/x-nacl"},
                        1: {type: "application/x-pnacl"},
                        description: "Native Client",
                        filename: "internal-nacl-plugin",
                        length: 2,
                        name: "Native Client"
                    }
                ]
            });
            
            // Имитация свойств Chrome
            window.chrome = {
                app: {
                    isInstalled: false,
                },
                webstore: {
                    onInstallStageChanged: {},
                    onDownloadProgress: {},
                },
                runtime: {
                    PlatformOs: {
                        MAC: 'mac',
                        WIN: 'win',
                        ANDROID: 'android',
                        CROS: 'cros',
                        LINUX: 'linux',
                        OPENBSD: 'openbsd',
                    },
                    PlatformArch: {
                        ARM: 'arm',
                        X86_32: 'x86-32',
                        X86_64: 'x86-64',
                    },
                    PlatformNaclArch: {
                        ARM: 'arm',
                        X86_32: 'x86-32',
                        X86_64: 'x86-64',
                    },
                    RequestUpdateCheckStatus: {
                        THROTTLED: 'throttled',
                        NO_UPDATE: 'no_update',
                        UPDATE_AVAILABLE: 'update_available',
                    },
                    OnInstalledReason: {
                        INSTALL: 'install',
                        UPDATE: 'update',
                        CHROME_UPDATE: 'chrome_update',
                        SHARED_MODULE_UPDATE: 'shared_module_update',
                    },
                    OnRestartRequiredReason: {
                        APP_UPDATE: 'app_update',
                        OS_UPDATE: 'os_update',
                        PERIODIC: 'periodic',
                    },
                }
            };
            """
            driver.execute_script(stealth_js)

            # Имитация реального поведения пользователя
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                Object.defineProperty(navigator, 'maxTouchPoints', {
                    get: () => 1
                });
                """
            })

            return driver

        except Exception as e:
            logger.error(f"Ошибка создания драйвера: {e}")
            if os.path.exists("proxy_auth_plugin.zip"):
                os.remove("proxy_auth_plugin.zip")
            raise

    def warm_up(self):
        """Прогрев браузера для имитации нормального поведения"""
        try:
            if not self.driver:
                return

            # Посещаем популярные сайты для имитации обычного поведения
            sites = ["https://ya.ru", "https://wikipedia.org"]

            for site in sites:
                try:
                    self.driver.get(site)
                    time.sleep(random.uniform(1, 3))

                    # Имитация скролла
                    self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight/4);")
                    time.sleep(random.uniform(0.5, 1.5))
                    self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight/2);")
                    time.sleep(random.uniform(0.5, 1))

                except Exception as e:
                    logger.warning(f"Ошибка при прогреве на {site}: {e}")
                    continue

            logger.info("Браузер успешно прогрет")

        except Exception as e:
            logger.warning(f"Ошибка при прогреве браузера: {e}")

    def quit(self):
        """Безопасное закрытие драйвера"""
        try:
            if self.driver:
                # Явно закрываем все окна браузера
                try:
                    self.driver.close()
                except Exception as close_ex:
                    logger.debug(f"Ошибка при закрытии окна: {close_ex}")
                
                # Завершаем процесс драйвера
                try:
                    self.driver.quit()
                except Exception as quit_ex:
                    logger.debug(f"Ошибка при завершении драйвера: {quit_ex}")
                
                self.driver = None
                logger.info("Драйвер успешно закрыт")
        except Exception as e:
            logger.warning(f"Ошибка при закрытии драйвера: {e}")

    def simulate_human_behavior(self):
        """Имитация человеческого поведения на странице"""
        if not self.driver:
            return
        try:
            # Все вызовы execute_script защищены проверкой
            self.driver.execute_script("...")
            try:
                # Случайные движения и скроллы
                viewport_height = self.driver.execute_script(
                    "return window.innerHeight")
                page_height = self.driver.execute_script(
                    "return document.body.scrollHeight")

                # Скролл вниз постепенно
                for i in range(1, 5):
                    scroll_to = min(i * viewport_height / 3,
                                    page_height - viewport_height)
                    self.driver.execute_script(f"window.scrollTo(0, {scroll_to});")
                    time.sleep(random.uniform(0.5, 1.5))

                # Имитация движения мыши через JavaScript
                self.driver.execute_script("""
                    // Создаем и диспатчим событие mousemove
                    function simulateMouseMove(x, y) {
                        const event = new MouseEvent('mousemove', {
                            'view': window,
                            'bubbles': true,
                            'cancelable': true,
                            'clientX': x,
                            'clientY': y
                        });
                        document.dispatchEvent(event);
                    }
                    
                    // Имитируем несколько движений мыши
                    for (let i = 0; i < 5; i++) {
                        const x = Math.floor(Math.random() * window.innerWidth);
                        const y = Math.floor(Math.random() * window.innerHeight);
                        simulateMouseMove(x, y);
                    }
                """)

                # Небольшая пауза после имитации
                time.sleep(random.uniform(1, 2))

            except Exception as e:
                logger.warning(f"Ошибка при имитации человеческого поведения: {e}")
                
        except Exception as e:
            logger.warning(f"Ошибка при выполнении скрипта: {e}")

    def parse_price(self, url: str) -> str | None:
        if not self.driver:
            try:
                self.driver = self.setup_driver()
                self.warm_up()
            except Exception as e:
                logger.error(f"Не удалось инициализировать драйвер: {e}")
                return None

        retries = 0
        while retries < Config.MAX_RETRIES:
            try:
                logger.info(f"Загрузка URL: {url}")
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
                    if price:
                        return price

                retries += 1
                self.rotate_identity()
            except Exception as e:
                logger.error(f"Ошибка при парсинге {url}: {e}")
                retries += 1
                self.rotate_identity()

        logger.error(f"Не удалось извлечь цену: {url}")
        return None

    def extract_price(self) -> str | None:
        if not self.driver:
            logger.error("Драйвер не инициализирован")
            return None
        
        """Извлечение цены со страницы различными методами"""
        # Метод 1: Проверка всех селекторов
        for selector in Config.PRICE_SELECTORS:
            try:
                # Ищем элемент с ожиданием
                wait = WebDriverWait(self.driver, 5)
                price_element = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                price_text = price_element.text.strip()

                # Проверяем, что текст похож на цену
                if '₽' in price_text or 'руб' in price_text:
                    logger.info(
                        f"Найдена цена по селектору {selector}: {price_text}")
                    return price_text
            except:
                continue

        # Метод 2: Поиск по регулярным выражениям в HTML
        page_source = self.driver.page_source

        for pattern in Config.PRICE_PATTERNS:
            matches = re.findall(pattern, page_source)
            if matches:
                price = matches[0] + " ₽"
                logger.info(f"Найдена цена по регулярному выражению: {price}")
                return price

        # Метод 3: Использование JavaScript для извлечения цены
        try:
            price_js = self.driver.execute_script("""
                // Найдем все элементы, которые могут содержать цену
                const priceElements = document.querySelectorAll('span, div');
                
                // Регулярное выражение для поиска цены
                const priceRegex = /(\\d+[\\s.]?\\d*)[\\s₽]+/;
                
                // Проходим по всем элементам
                for (const el of priceElements) {
                    if (el.innerText && 
                        (el.innerText.includes('₽') || el.innerText.includes('руб')) && 
                        priceRegex.test(el.innerText)) {
                        return el.innerText.trim();
                    }
                }
                
                return null;
            """)

            if price_js:
                logger.info(f"Найдена цена с помощью JavaScript: {price_js}")
                return price_js
        except:
            pass

        return None

    def save_page_source(self, url):
        if not self.driver:
            return
        """Сохраняет HTML страницы для отладки"""
        try:
            if not self.driver:
                return

            # Создаем папку для отладочных файлов
            if not os.path.exists("debug"):
                os.makedirs("debug")

            # Формируем имя файла из URL
            filename = "debug/page_" + str(hash(url))[-8:] + ".html"

            with open(filename, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)

            logger.info(f"HTML сохранен в {filename}")

# Делаем скриншот
            screenshot_file = "debug/screen_" + str(hash(url))[-8:] + ".png"
            self.driver.save_screenshot(screenshot_file)
            logger.info(f"Скриншот сохранен в {screenshot_file}")

        except Exception as e:
            logger.warning(f"Ошибка при сохранении страницы: {e}")

    def is_blocked(self) -> bool:
        if not self.driver:
            return False
        """Проверяет наличие страницы блокировки"""
        try:
            # Проверка на различные формы блокировки
            blocks = [
                # Общая форма блокировки
                "//div[contains(@class, 'fab-chlg')]",
                # Текст капчи
                "//div[contains(text(), 'Подтвердите, что вы не робот')]",
                # Сообщение об ограничении
                "//span[contains(text(), 'Доступ ограничен')]",
                # Контейнер безопасности
                "//div[contains(@class, 'security-container')]",
                "//iframe[contains(@src, 'captcha')]",  # iFrame с капчей
                "//div[contains(@class, 'captcha')]"  # Элемент капчи
            ]

            for xpath in blocks:
                try:
                    element = self.driver.find_element(By.XPATH, xpath)
                    if element.is_displayed():
                        logger.warning(f"Обнаружена блокировка: {xpath}")
                        return True
                except:
                    continue

            # Дополнительная проверка через JavaScript
            is_blocked_js = self.driver.execute_script("""
                return document.body.innerText.includes('робот') || 
                       document.body.innerText.includes('captcha') ||
                       document.body.innerText.includes('капча') ||
                       document.body.innerText.includes('Доступ ограничен') ||
                       document.body.innerText.includes('безопасность') ||
                       document.body.innerText.includes('Подтвердите');
            """)

            if is_blocked_js:
                logger.warning(
                    "Обнаружена блокировка через JavaScript проверку")
                return True

            return False

        except Exception as e:
            logger.warning(f"Ошибка при проверке блокировки: {e}")
            return False

    def handle_block(self):
        if not self.driver:
            return
        
        """Обработка страницы защиты Ozon"""
        try:
            logger.info("Попытка обхода блокировки...")
            self.anti_bot_counter += 1

            # Шаг 1: Проверяем наличие кнопки обновления
            try:
                update_buttons = [
                    "//button[contains(text(), 'Обновить')]",
                    "//button[contains(text(), 'Продолжить')]",
                    "//button[contains(text(), 'Подтвердить')]",
                    "//button[contains(@class, 'refresh')]",
                    "//div[contains(@class, 'button')][contains(text(), 'Обновить')]"
                ]

                for xpath in update_buttons:
                    try:
                        button = WebDriverWait(self.driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, xpath))
                        )
                        button.click()
                        logger.info(f"Нажата кнопка обновления: {xpath}")
                        time.sleep(5)  # Ждем после нажатия
                        return  # Если успешно нажали, выходим
                    except:
                        continue
            except Exception as e:
                logger.warning(f"Не удалось найти кнопку обновления: {e}")

            # Шаг 2: Если счетчик превысил порог, меняем прокси и User-Agent
            if self.anti_bot_counter >= 2:
                logger.warning(
                    "Частые блокировки. Полная смена идентификации...")
                self.rotate_identity()
                time.sleep(Config.PROXY_CHANGE_DELAY)
                return

            # Шаг 3: Попытка обойти блокировку через JavaScript
            try:
                bypass_js = """
                // Попытка автоматического клика на кнопку
                const buttons = document.querySelectorAll('button');
                for (const button of buttons) {
                    if (button.innerText.includes('Обновить') || 
                        button.innerText.includes('Продолжить') ||
                        button.innerText.includes('Подтвердить')) {
                        button.click();
                        return true;
                    }
                }
                
                // Пробуем найти элементы с классами для кликов
                const clickables = document.querySelectorAll('.button, .btn, [role="button"]');
                for (const el of clickables) {
                    if (el.innerText.includes('Обновить') || 
                        el.innerText.includes('Продолжить') ||
                        el.innerText.includes('Подтвердить')) {
                        el.click();
                        return true;
                    }
                }
                
                return false;
                """
                result = self.driver.execute_script(bypass_js)
                if result:
                    logger.info("Выполнен JavaScript обход блокировки")
                    time.sleep(5)  # Ждем после нажатия
                    return
            except Exception as e:
                logger.warning(f"Ошибка при JavaScript обходе: {e}")

            # Шаг 4: Если ничего не помогло, делаем полную перезагрузку
            logger.warning(
                "Не удалось обойти блокировку. Полная перезагрузка...")
            self.rotate_identity()

        except Exception as e:
            logger.error(f"Ошибка обхода защиты: {e}")
            # Если произошла серьезная ошибка, меняем прокси
            self.rotate_identity()

    def rotate_identity(self):
        """Полная смена идентификации (прокси и User-Agent)"""
        try:
            logger.info("Смена идентификации...")

            # Сохраняем текущий URL если есть
            current_url = None
            if self.driver:
                try:
                    current_url = self.driver.current_url
                except:
                    pass

            # Закрываем текущий драйвер
            self.quit()

            # Получаем новый прокси и User-Agent
            self.proxy_info = self.proxy_manager.get_proxy()
            self.user_agent = self.proxy_manager.get_random_user_agent()

            # Ждем перед новым подключением
            time.sleep(Config.PROXY_CHANGE_DELAY)

            # Создаем новый драйвер
            self.driver = self.setup_driver()

            # Прогреваем браузер
            self.warm_up()

            logger.success(
                f"Идентификация успешно изменена на {self.proxy_info[0]}")

            # Сбрасываем счетчик блокировок
            self.anti_bot_counter = 0

        except Exception as e:
            logger.error(f"Ошибка при смене идентификации: {e}")


class ThreadManager:
    def __init__(self, urls: list, proxy_manager: ProxyManager):
        self.url_queue = Queue()
        for url in urls:
            self.url_queue.put(url)
        self.proxy_manager = proxy_manager
        self.results = {}
        self.lock = Lock()
        self.failed_urls = []  # Добавляем список для неудачных URL

    def worker(self):
        parser = None
        while not self.url_queue.empty():
            try:
                url = self.url_queue.get()

                # Создаем новый парсер, если нет или драйвер не инициализирован
                if not parser or not parser.driver:
                    parser = Parser(self.proxy_manager)

                # Парсим цену
                logger.info(f"Обработка URL: {url}")
                price = parser.parse_price(url)

                # Сохраняем результат
                with self.lock:
                    if price:
                        self.results[url] = price
                        logger.success(
                            f"Успешно получена цена для {url}: {price}")
                    else:
                        self.failed_urls.append(url)
                        logger.error(f"Не удалось получить цену для {url}")
                

            except Exception as e:
                logger.error(f"Ошибка в потоке: {e}")
                with self.lock:
                    self.failed_urls.append(url)

                # Безопасное завершение текущего парсера
                if parser:
                    try:
                        parser.quit()
                    except:
                        pass

                # Небольшая пауза перед повторной попыткой
                time.sleep(Config.PROXY_CHANGE_DELAY)

                # Перезапуск с новым парсером
                parser = None

            finally:
                self.url_queue.task_done()
                # Делаем паузу между запросами
                time.sleep(random.uniform(*Config.REQUEST_DELAY))

        # Закрываем браузер после работы
        if parser:
            parser.quit()

    def start(self):
        """Запуск потоков с учётом MAX_PROXIES и THREADS_PER_PROXY"""
        threads = []
        total_threads = min(Config.THREADS_PER_PROXY * len(self.proxy_manager.proxies),
                            Config.MAX_PROXIES * Config.THREADS_PER_PROXY)

        # Защита от 0 потоков
        total_threads = max(1, total_threads)

        logger.info(
            f"Запуск {total_threads} потоков для обработки {self.url_queue.qsize()} URL")

        for i in range(total_threads):
            thread = Thread(target=self.worker, name=f"Parser-{i+1}")
            threads.append(thread)
            thread.daemon = True  # Делаем потоки демонами для автоматического завершения
            thread.start()
            # Небольшая задержка между запуском потоков
            time.sleep(1)

        # Ожидание завершения всех задач
        self.url_queue.join()

        # Информация о результатах
        logger.info(
            f"Обработка завершена. Успешно: {len(self.results)}, Неудачно: {len(self.failed_urls)}")

        # Если есть неудачные URL, делаем повторную попытку
        # Ограничиваем повторные попытки
        if self.failed_urls and len(self.failed_urls) < 10:
            logger.info(
                f"Повторная попытка для {len(self.failed_urls)} неудачных URL")

            # Помещаем неудачные URL обратно в очередь
            retry_urls = self.failed_urls.copy()
            self.failed_urls = []

            for url in retry_urls:
                self.url_queue.put(url)

            # Запускаем потоки снова
            for i in range(min(total_threads, len(retry_urls))):
                thread = Thread(target=self.worker, name=f"Retry-{i+1}")
                threads.append(thread)
                thread.daemon = True
                thread.start()
                time.sleep(1)

            # Ожидание завершения повторных попыток
            self.url_queue.join()


def main():
    # Настройка логирования
    if not os.path.exists("logs"):
        os.makedirs("logs")

    log_file = f"logs/parser_link_{time.strftime('%Y%m%d_%H%M%S')}.log"

    logger.remove()  # Удаляем стандартный обработчик
    logger.add(sys.stderr, level="INFO")  # Логи в консоль
    logger.add(log_file,  # Логи в файл
               format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function}:{line} - {message}",
               level="DEBUG",
               rotation="10 MB",
               retention="1 week")

    logger.info("=" * 50)
    logger.info("Старт парсера Ozon")
    logger.info("=" * 50)

    # Создание нужных папок
    for folder in ["in", "out", "debug"]:
        if not os.path.exists(folder):
            os.makedirs(folder)
            logger.info(f"Создана папка {folder}")

    # Загрузка данных
    try:
        input_filename = "in/1_1_product.xlsx"  # Единое имя файла

        if not os.path.exists(input_filename):
            logger.error(f"Файл '{input_filename}' не найден!")
            return

        df = pd.read_excel(input_filename)

        # Проверка наличия нужной колонки
        if "Ссылка на товар" not in df.columns:
            logger.error(
                "В файле Excel отсутствует колонка 'Ссылка на товар'!")
            return

        # Очистка и проверка URL
        urls = df["Ссылка на товар"].dropna().tolist()
        valid_urls = []

        for url in urls:
            # Проверка и корректировка URL
            url = str(url).strip()
            if not url.startswith("http"):
                url = f"https://{url}"
            if "ozon.ru" not in url:
                logger.warning(f"Пропущен неподходящий URL: {url}")
                continue

            valid_urls.append(url)

        if not valid_urls:
            logger.error("Нет валидных URL для обработки!")
            return

        logger.info(f"Загружено {len(valid_urls)} валидных URL")

    except Exception as e:
        logger.error(f"Ошибка чтения Excel: {e}")
        return

    # Инициализация прокси-менеджера
    try:
        proxy_manager = ProxyManager()
        if not proxy_manager.proxies:
            logger.error("Не удалось инициализировать прокси!")
            return
    except Exception as e:
        logger.error(f"Ошибка инициализации прокси: {e}")
        return

    # Запуск парсинга
    try:
        thread_manager = ThreadManager(valid_urls, proxy_manager)
        thread_manager.start()
    except Exception as e:
        logger.error(f"Ошибка запуска потоков: {e}")
        return

    # Сохранение результатов
    try:
        df["Цена по карте озон"] = df["Ссылка на товар"].map(thread_manager.results)
        df["Дата парсинга"] = time.strftime("%Y-%m-%d %H:%M:%S")

        output_file = f"out/result_price_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        df.to_excel(output_file, index=False)

        # Статистика
        success_count = len(thread_manager.results)
        failed_count = len(thread_manager.failed_urls)
        total_count = len(valid_urls)
        success_rate = (success_count / total_count) * \
            100 if total_count > 0 else 0

        logger.success(f"Парсинг завершен! Статистика:")
        logger.success(f"Обработано URL: {total_count}")
        logger.success(f"Успешно: {success_count} ({success_rate:.1f}%)")
        logger.success(f"Неудачно: {failed_count}")
        logger.success(f"Результаты сохранены в {output_file}")
    
    except Exception as e:
        logger.error(f"Ошибка сохранения результатов: {e}")
        
    # print total traffic
    logger.info(f"Total traffic used: {traffic_monitor.get_total_traffic()}")

    logger.info("=" * 50)
    logger.info("Парсер завершил работу")
    logger.info("=" * 50)


if __name__ == "__main__":

    main()
