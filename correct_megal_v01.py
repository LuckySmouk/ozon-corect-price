import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, filedialog
import asyncio
import aiohttp
import pandas as pd
import random
from datetime import datetime
from loguru import logger
from conf import BASE_URL, CLIENT_ID, API_KEY
import json
import os


class Config:
    BASE_URL = BASE_URL.rstrip('/')
    CLIENT_ID = CLIENT_ID
    API_KEY = API_KEY
    REQUEST_TIMEOUT = 30
    PRODUCT_DELAY_RANGE = (4, 15)
conf = Config()


class OzonAPI:
    """Класс для работы с API Ozon"""
    
    def __init__(self, session):
        self.session = session
    
    async def api_request(self, method, endpoint, json_payload=None):
        """Асинхронный запрос к API с обработкой ошибок"""
        if not self.session or self.session.closed:
            return None
            
        url = f"{conf.BASE_URL}{endpoint}"
        start_time = datetime.now()
        logger.debug(f"API-запрос к {endpoint} начат")

        for attempt in range(1, 6):
            try:
                async with self.session.request(method, url, json=json_payload, ssl=False) as resp:
                    text = await resp.text()
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.debug(f"API-запрос к {endpoint} завершен за {elapsed:.2f} секунд")

                    if resp.status == 200:
                        try:
                            result = await resp.json()
                            return result
                        except aiohttp.ContentTypeError:
                            logger.error(f"Невалидный JSON в ответе: {text}")
                            return None

                    if resp.status in (429, 500, 502, 503, 504):
                        logger.warning(f"{endpoint} попытка {attempt} вернула {resp.status}. Повтор...")
                        delay = 2 ** attempt + random.random()
                        await asyncio.sleep(delay)
                        continue

                    logger.error(f"API {endpoint} ошибка {resp.status}: {text}")
                    return None

            except Exception as e:
                logger.warning(f"{endpoint} попытка {attempt} вызвала исключение: {e}")
                delay = 2 ** attempt + random.random()
                await asyncio.sleep(delay)

        logger.error(f"API {endpoint} превышено количество попыток")
        return None

    async def get_product_info(self, product_ids):
        """Получение информации о товарах"""
        if isinstance(product_ids, int):
            product_ids = [str(product_ids)]
        elif isinstance(product_ids, list):
            product_ids = [str(pid) for pid in product_ids]
            
        payload = {"product_id": product_ids}
        return await self.api_request('POST', '/v3/product/info/list', payload)

    async def get_actions(self):
        """Получение списка всех доступных акций"""
        data = await self.api_request('GET', '/v1/actions')
        actions = {}
        if data and 'result' in data:
            for act in data['result']:
                if 'title' in act and 'id' in act:
                    actions[act['title']] = act['id']
            logger.info(f"Получено {len(actions)} акций")
        return actions

    async def deactivate_actions(self, product_id, actions, titles):
        """Деактивация акций для товара"""
        deactivated = []
        for title in titles:
            aid = actions.get(title)
            if not aid:
                continue

            payload = {'action_id': aid, 'product_ids': [product_id]}
            data = await self.api_request('POST', '/v1/actions/products/deactivate', payload)

            if data and product_id in data.get('result', {}).get('product_ids', []):
                deactivated.append(title)
                logger.info(f"Товар {product_id} деактивирован из '{title}'")

        return deactivated

    async def activate_actions(self, product_id, actions, titles, action_price):
        """Активация акций для товара"""
        activated = []
        for title in titles:
            aid = actions.get(title)
            if not aid:
                continue

            payload = {
                'action_id': aid,
                'products': [{'product_id': product_id, 'action_price': action_price, 'stock': 10}]
            }

            data = await self.api_request('POST', '/v1/actions/products/activate', payload)

            if data and product_id in data.get('result', {}).get('product_ids', []):
                activated.append(title)
                logger.info(f"Товар {product_id} активирован в '{title}'")

        return activated

    async def update_prices(self, prices_data):
        """Обновление цен товаров"""
        return await self.api_request('POST', '/v1/product/import/prices', prices_data)


class ProductManager:
    """Менеджер для работы с товарами"""
    
    def __init__(self, api):
        self.api = api
        self.products = {}
        self.cached_actions = None
    
    async def load_products(self, product_ids):
        """Загрузка информации о товарах"""
        if not product_ids:
            return []
            
        data = await self.api.get_product_info(product_ids)
        products_info = []
        
        if data and 'items' in data:
            for item in data['items']:
                product_id = item.get('id')
                if product_id:
                    product_info = {
                        'id': product_id,
                        'offer_id': item.get('offer_id', ''),
                        'base_price': item.get('price', '0'),
                        'old_price': item.get('old_price', '0'),
                        'min_price': item.get('min_price', '0'),
                        'currency_code': item.get('currency_code', 'RUB'),
                        'marketing_actions': self._parse_marketing_actions(item.get('marketing_actions', {}))
                    }
                    self.products[product_id] = product_info
                    products_info.append(product_info)
                    
        return products_info
    
    def _parse_marketing_actions(self, marketing_info):
        """Парсинг информации о маркетинговых акциях"""
        if isinstance(marketing_info, dict):
            return marketing_info.get('actions', [])
        elif isinstance(marketing_info, list):
            return marketing_info
        return []
    
    async def get_actions(self):
        """Получение списка акций с кэшированием"""
        if self.cached_actions is None:
            self.cached_actions = await self.api.get_actions()
        return self.cached_actions
    
    def clear_actions_cache(self):
        """Очистка кэша акций"""
        self.cached_actions = None


class ProductEditDialog(tk.Toplevel):
    """Диалоговое окно редактирования товара"""
    
    def __init__(self, parent, product_info, product_manager, loop):
        super().__init__(parent)
        self.parent = parent
        self.product_info = product_info
        self.product_manager = product_manager
        self.loop = loop
        
        self.title(f"Редактирование товара {product_info['id']}")
        self.geometry("1000x700")
        self.resizable(True, True)
        
        self.create_ui()
        self.load_data()
        
    def create_ui(self):
        """Создание интерфейса"""
        # Notebook для вкладок
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Вкладка цен
        self.create_prices_tab()
        
        # Вкладка акций
        self.create_actions_tab()
        
        # Кнопки управления
        button_frame = ttk.Frame(self)
        button_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Button(button_frame, text="Применить изменения", 
                  command=self.apply_changes).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Отмена", 
                  command=self.destroy).pack(side=tk.RIGHT, padx=5)
    
    def create_prices_tab(self):
        """Создание вкладки управления ценами"""
        prices_frame = ttk.Frame(self.notebook)
        self.notebook.add(prices_frame, text="Цены")
        
        # Таблица цен
        tree_frame = ttk.Frame(prices_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.prices_tree = ttk.Treeview(tree_frame, columns=("Parameter", "Value", "NewValue"), 
                                       show="headings", height=8)
        self.prices_tree.heading("Parameter", text="Параметр")
        self.prices_tree.heading("Value", text="Текущее значение")
        self.prices_tree.heading("NewValue", text="Новое значение")
        self.prices_tree.column("Parameter", width=200)
        self.prices_tree.column("Value", width=150)
        self.prices_tree.column("NewValue", width=150)
        
        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.prices_tree.yview)
        self.prices_tree.configure(yscrollcommand=scrollbar.set)
        
        self.prices_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Кнопки управления ценами
        price_buttons = ttk.Frame(prices_frame)
        price_buttons.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Button(price_buttons, text="Изменить выбранную цену", 
                  command=self.edit_selected_price).pack(side=tk.LEFT, padx=5)
        ttk.Button(price_buttons, text="Обновить все цены", 
                  command=self.update_all_prices).pack(side=tk.LEFT, padx=5)
    
    def create_actions_tab(self):
        """Создание вкладки управления акциями"""
        actions_frame = ttk.Frame(self.notebook)
        self.notebook.add(actions_frame, text="Акции")
        
        # Подвкладки для акций
        sub_notebook = ttk.Notebook(actions_frame)
        sub_notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Активные акции
        active_frame = ttk.Frame(sub_notebook)
        sub_notebook.add(active_frame, text="Активные акции")
        
        self.active_tree = ttk.Treeview(active_frame, columns=("Action"), show="headings")
        self.active_tree.heading("Action", text="Акция")
        self.active_tree.column("Action", width=400)
        self.active_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar_active = ttk.Scrollbar(active_frame, orient=tk.VERTICAL, command=self.active_tree.yview)
        self.active_tree.configure(yscrollcommand=scrollbar_active.set)
        scrollbar_active.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Доступные акции
        available_frame = ttk.Frame(sub_notebook)
        sub_notebook.add(available_frame, text="Доступные акции")
        
        self.available_tree = ttk.Treeview(available_frame, columns=("Action"), show="headings")
        self.available_tree.heading("Action", text="Акция")
        self.available_tree.column("Action", width=400)
        self.available_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar_available = ttk.Scrollbar(available_frame, orient=tk.VERTICAL, command=self.available_tree.yview)
        self.available_tree.configure(yscrollcommand=scrollbar_available.set)
        scrollbar_available.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Кнопки управления акциями
        action_buttons = ttk.Frame(actions_frame)
        action_buttons.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Button(action_buttons, text="Деактивировать выбранную", 
                  command=self.deactivate_selected_action).pack(side=tk.LEFT, padx=5)
        ttk.Button(action_buttons, text="Активировать выбранную", 
                  command=self.activate_selected_action).pack(side=tk.LEFT, padx=5)
        ttk.Button(action_buttons, text="Обновить список акций", 
                  command=self.refresh_actions).pack(side=tk.LEFT, padx=5)
    
    def load_data(self):
        """Загрузка данных о товаре"""
        self.load_prices()
        self.load_actions()
    
    def load_prices(self):
        """Загрузка информации о ценах"""
        self.prices_tree.delete(*self.prices_tree.get_children())
        
        price_fields = [
            ('base_price', 'Базовая цена'),
            ('old_price', 'Старая цена'), 
            ('min_price', 'Минимальная цена'),
            ('currency_code', 'Валюта')
        ]
        
        for field, display_name in price_fields:
            value = str(self.product_info.get(field, ''))
            self.prices_tree.insert("", tk.END, values=(display_name, value, ""))
    
    async def _load_actions_data(self):
        """Асинхронная загрузка данных об акциях"""
        all_actions = await self.product_manager.get_actions()
        active_titles = [action.get('title', '') for action in self.product_info.get('marketing_actions', [])]
        
        return all_actions, active_titles
    
    def load_actions(self):
        """Загрузка информации об акциях"""
        all_actions, active_titles = self.loop.run_until_complete(self._load_actions_data())
        
        self.active_tree.delete(*self.active_tree.get_children())
        for title in active_titles:
            self.active_tree.insert("", tk.END, values=(title,))
        
        self.available_tree.delete(*self.available_tree.get_children())
        for title in all_actions.keys():
            if title not in active_titles:
                self.available_tree.insert("", tk.END, values=(title,))
    
    def edit_selected_price(self):
        """Редактирование выбранной цены"""
        selection = self.prices_tree.selection()
        if not selection:
            messagebox.showwarning("Предупреждение", "Выберите цену для изменения")
            return
        
        item = self.prices_tree.item(selection[0])
        param_display, old_value, _ = item['values']
        
        # Сопоставление отображаемых имен с именами полей
        param_map = {
            'Базовая цена': 'base_price',
            'Старая цена': 'old_price', 
            'Минимальная цена': 'min_price',
            'Валюта': 'currency_code'
        }
        
        param_field = param_map.get(param_display)
        if not param_field:
            return
            
        new_value = simpledialog.askstring(
            "Изменение цены",
            f"Введите новое значение для {param_display}:\nТекущее значение: {old_value}",
            initialvalue=old_value
        )
        
        if new_value is not None and new_value != old_value:
            self.prices_tree.set(selection[0], "NewValue", new_value)
            self.product_info[param_field] = new_value
    
    def update_all_prices(self):
        """Обновление всех цен"""
        dialog = PriceUpdateDialog(self, "Обновление всех цен", self.product_info)
        if dialog.result:
            for field, value in dialog.result.items():
                if field in self.product_info:
                    self.product_info[field] = value
            self.load_prices()
    
    def deactivate_selected_action(self):
        """Деактивация выбранной акции"""
        selection = self.active_tree.selection()
        if not selection:
            messagebox.showwarning("Предупреждение", "Выберите акцию для деактивации")
            return
            
        selected_action = self.active_tree.item(selection[0])['values'][0]
        product_id = self.product_info['id']
        
        async def deactivate():
            all_actions = await self.product_manager.get_actions()
            result = await self.product_manager.api.deactivate_actions(
                product_id, all_actions, [selected_action]
            )
            return result
        
        result = self.loop.run_until_complete(deactivate())
        if result:
            messagebox.showinfo("Успех", f"Акция '{selected_action}' деактивирована")
            self.load_actions()
        else:
            messagebox.showerror("Ошибка", "Не удалось деактивировать акцию")
    
    def activate_selected_action(self):
        """Активация выбранной акции"""
        selection = self.available_tree.selection()
        if not selection:
            messagebox.showwarning("Предупреждение", "Выберите акцию для активации")
            return

        selected_action = self.available_tree.item(selection[0])['values'][0]
        product_id = self.product_info['id']

        action_price = simpledialog.askinteger(
            "Цена акции",
            "Введите цену для активации в акции:",
            initialvalue=1000,
            minvalue=1,
            maxvalue=1000000
        )

        if action_price is None:
            return

        async def activate():
            all_actions = await self.product_manager.get_actions()
            result = await self.product_manager.api.activate_actions(
                product_id, all_actions, [selected_action], action_price
            )
            return result

        result = self.loop.run_until_complete(activate())
        if result:
            messagebox.showinfo("Успех", f"Акция '{result[0]}' активирована")
            self.load_actions()
        else:
            messagebox.showerror("Ошибка", "Не удалось активировать акцию")
    
    def refresh_actions(self):
        """Обновление списка акций"""
        self.product_manager.clear_actions_cache()
        self.load_actions()
    
    def apply_changes(self):
        """Применение изменений"""
        # Обновление цен через API
        offer_id = self.product_info.get('offer_id')
        if offer_id:
            payload = {
                'prices': [{
                    'offer_id': offer_id,
                    'old_price': str(self.product_info.get('old_price', 0)),
                    'price': str(self.product_info.get('base_price', 0)),
                    'min_price': str(self.product_info.get('min_price', 0)),
                    'currency_code': self.product_info.get('currency_code', 'RUB'),
                    'auto_action_enabled': 'UNKNOWN',
                    'auto_add_to_ozon_actions_list_enabled': 'UNKNOWN',
                    'price_strategy_enabled': 'DISABLED',
                    'min_price_for_auto_actions_enabled': True
                }]
            }
            
            async def update_prices():
                return await self.product_manager.api.update_prices(payload)
            
            result = self.loop.run_until_complete(update_prices())
            if result:
                messagebox.showinfo("Успех", "Изменения применены успешно")
                self.destroy()
            else:
                messagebox.showerror("Ошибка", "Не удалось применить изменения")
        else:
            messagebox.showerror("Ошибка", "Не найден offer_id для товара")


class PriceUpdateDialog(simpledialog.Dialog):
    """Диалоговое окно для обновления всех цен"""
    
    def __init__(self, parent, title, current_prices):
        self.current_prices = current_prices
        self.result = None
        super().__init__(parent, title)
    
    def body(self, master):
        """Создание элементов управления"""
        self.entries = {}
        row = 0
        
        price_fields = [
            ('base_price', 'Базовая цена'),
            ('old_price', 'Старая цена'),
            ('min_price', 'Минимальная цена'),
            ('currency_code', 'Валюта')
        ]
        
        for field, display_name in price_fields:
            ttk.Label(master, text=display_name).grid(row=row, column=0, padx=5, pady=5, sticky='w')
            entry = ttk.Entry(master, width=30)
            entry.grid(row=row, column=1, padx=5, pady=5)
            entry.insert(0, str(self.current_prices.get(field, '')))
            self.entries[field] = entry
            row += 1
            
        return self.entries[list(self.entries.keys())[0]]
    
    def apply(self):
        """Применение изменений"""
        self.result = {}
        for key, entry in self.entries.items():
            self.result[key] = entry.get()


class OzonProductManager:
    """Главное приложение для управления товарами Ozon"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Ozon Product Manager")
        self.root.geometry("1200x700")
        
        # Инициализация асинхронного цикла
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Инициализация API и менеджера товаров
        self.session = None
        self.api = None
        self.product_manager = None
        
        # Данные приложения
        self.products = {}
        
        self.create_ui()
        # Отложенная инициализация сессии до первого использования
        self.initialized = False
        
    async def ensure_initialized(self):
        """Гарантирует, что сессия инициализирована"""
        if self.initialized and self.session and not self.session.closed:
            return True
            
        try:
            await self.init_session()
            self.initialized = True
            return True
        except Exception as e:
            logger.error(f"Ошибка инициализации сессии: {e}")
            messagebox.showerror("Ошибка", f"Не удалось инициализировать сессию: {e}")
            return False
    
    async def init_session(self):
        """Инициализация асинхронной сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
            
        headers = {
            'Client-Id': conf.CLIENT_ID,
            'Api-Key': conf.API_KEY,
            'Content-Type': 'application/json'
        }
        timeout = aiohttp.ClientTimeout(total=conf.REQUEST_TIMEOUT)
        self.session = aiohttp.ClientSession(headers=headers, timeout=timeout)
        self.api = OzonAPI(self.session)
        self.product_manager = ProductManager(self.api)
        logger.info("Сессия успешно создана")
    
    def create_ui(self):
        """Создание пользовательского интерфейса"""
        # Панель управления
        control_frame = ttk.Frame(self.root, padding="10")
        control_frame.pack(fill=tk.X)
        
        ttk.Label(control_frame, text="Ozon Product ID:").pack(side=tk.LEFT)
        self.product_id_entry = ttk.Entry(control_frame, width=20)
        self.product_id_entry.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(control_frame, text="Добавить товар", 
                  command=self.add_single_product).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(control_frame, text="Загрузить из файла", 
                  command=self.load_from_file).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(control_frame, text="Загрузить из Excel", 
                  command=self.load_from_excel).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(control_frame, text="Очистить список", 
                  command=self.clear_products).pack(side=tk.LEFT, padx=5)
        
        # Таблица товаров
        table_frame = ttk.Frame(self.root)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        columns = ("ID", "Base Price", "Old Price", "Min Price", "Currency")
        self.products_tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=15)
        
        # Настройка колонок
        self.products_tree.heading("ID", text="Ozon Product ID")
        self.products_tree.heading("Base Price", text="Базовая цена")
        self.products_tree.heading("Old Price", text="Старая цена") 
        self.products_tree.heading("Min Price", text="Минимальная цена")
        self.products_tree.heading("Currency", text="Валюта")
        
        self.products_tree.column("ID", width=150)
        self.products_tree.column("Base Price", width=120)
        self.products_tree.column("Old Price", width=120)
        self.products_tree.column("Min Price", width=120)
        self.products_tree.column("Currency", width=80)
        
        # Скроллбар для таблицы
        scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.products_tree.yview)
        self.products_tree.configure(yscrollcommand=scrollbar.set)
        
        self.products_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Двойной клик для редактирования
        self.products_tree.bind("<Double-1>", self.on_double_click)
        
        # Панель действий
        action_frame = ttk.Frame(self.root, padding="10")
        action_frame.pack(fill=tk.X)
        
        ttk.Button(action_frame, text="Обновить данные", 
                  command=self.refresh_products).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(action_frame, text="Редактировать выбранный", 
                  command=self.edit_selected_product).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(action_frame, text="Экспорт в CSV", 
                  command=self.export_to_csv).pack(side=tk.LEFT, padx=5)
        
        # Статус бар
        self.status_var = tk.StringVar()
        self.status_var.set("Готов к работе")
        status_bar = ttk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)
    
    def add_single_product(self):
        """Добавление одного товара по ID"""
        product_id = self.product_id_entry.get().strip()
        if not product_id:
            messagebox.showwarning("Предупреждение", "Введите Product ID")
            return
        
        try:
            product_id = int(product_id)
        except ValueError:
            messagebox.showerror("Ошибка", "Product ID должен быть числом")
            return
        
        self.add_products([product_id])
        self.product_id_entry.delete(0, tk.END)
    
    def load_from_file(self):
        """Загрузка товаров из текстового файла"""
        filename = filedialog.askopenfilename(
            title="Выберите текстовый файл",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        
        if not filename:
            return
        
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                product_ids = []
                for line in f:
                    line = line.strip()
                    if line and line.isdigit():
                        product_ids.append(int(line))
                
                if product_ids:
                    self.add_products(product_ids)
                else:
                    messagebox.showwarning("Предупреждение", "Не найдено валидных Product ID в файле")
                    
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка чтения файла: {str(e)}")
    
    def load_from_excel(self):
        """Загрузка товаров из Excel файла"""
        filename = filedialog.askopenfilename(
            title="Выберите Excel файл",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
        )
        
        if not filename:
            return
        
        try:
            df = pd.read_excel(filename)
            
            # Поиск колонки с Product ID
            id_columns = [col for col in df.columns if 'ozon product id' in str(col).lower()]
            
            if not id_columns:
                messagebox.showerror("Ошибка", "Не найдена колонка 'Ozon Product ID' в файле")
                return
            
            product_ids = df[id_columns[0]].dropna().astype(int).tolist()
            
            if product_ids:
                self.add_products(product_ids)
            else:
                messagebox.showwarning("Предупреждение", "Не найдено валидных Product ID в файле")
                
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка чтения Excel файла: {str(e)}")
    
    def add_products(self, product_ids):
        """Добавление товаров в список"""
        if not product_ids:
            return
            
        # Гарантируем инициализацию перед использованием
        if not self.loop.run_until_complete(self.ensure_initialized()):
            return
            
        if not self.product_manager:
            messagebox.showerror("Ошибка", "Менеджер товаров не инициализирован")
            return
            
        self.status_var.set("Загрузка данных о товарах...")
        
        async def load_products():
            return await self.product_manager.load_products(product_ids)
        
        try:
            products_info = self.loop.run_until_complete(load_products())
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка загрузки товаров: {str(e)}")
            self.status_var.set("Ошибка загрузки")
            return
        
        for product in products_info:
            product_id = product['id']
            self.products[product_id] = product
            
            # Добавляем или обновляем в таблице
            existing = False
            for item in self.products_tree.get_children():
                if self.products_tree.item(item)['values'][0] == product_id:
                    self.products_tree.item(item, values=(
                        product_id,
                        product['base_price'],
                        product['old_price'], 
                        product['min_price'],
                        product['currency_code']
                    ))
                    existing = True
                    break
            
            if not existing:
                self.products_tree.insert("", tk.END, values=(
                    product_id,
                    product['base_price'],
                    product['old_price'],
                    product['min_price'], 
                    product['currency_code']
                ))
        
        self.status_var.set(f"Загружено {len(products_info)} товаров")
    
    def clear_products(self):
        """Очистка списка товаров"""
        self.products_tree.delete(*self.products_tree.get_children())
        self.products.clear()
        self.status_var.set("Список товаров очищен")
    
    def refresh_products(self):
        """Обновление данных о товарах"""
        product_ids = list(self.products.keys())
        if product_ids:
            self.add_products(product_ids)
            self.status_var.set("Данные обновлены")
        else:
            messagebox.showinfo("Информация", "Нет товаров для обновления")
    
    def on_double_click(self, event):
        """Обработка двойного клика по товару"""
        self.edit_selected_product()
    
    def edit_selected_product(self):
        """Редактирование выбранного товара"""
        selection = self.products_tree.selection()
        if not selection:
            messagebox.showwarning("Предупреждение", "Выберите товар для редактирования")
            return
        
        product_id = self.products_tree.item(selection[0])['values'][0]
        product_info = self.products.get(product_id)
        
        if product_info:
            # Гарантируем инициализацию перед открытием диалога
            if not self.loop.run_until_complete(self.ensure_initialized()):
                return
                
            dialog = ProductEditDialog(self.root, product_info, self.product_manager, self.loop)
            self.root.wait_window(dialog)
            
            # Обновляем данные в таблице после редактирования
            if product_id in self.products:
                product = self.products[product_id]
                self.products_tree.item(selection[0], values=(
                    product_id,
                    product['base_price'],
                    product['old_price'],
                    product['min_price'],
                    product['currency_code']
                ))
        else:
            messagebox.showerror("Ошибка", "Информация о товаре не найдена")
    
    def export_to_csv(self):
        """Экспорт данных в CSV файл"""
        if not self.products:
            messagebox.showwarning("Предупреждение", "Нет данных для экспорта")
            return
        
        filename = filedialog.asksaveasfilename(
            title="Экспорт в CSV",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        
        if not filename:
            return
        
        try:
            data = []
            for product_id, product in self.products.items():
                data.append({
                    'Ozon Product ID': product_id,
                    'Base Price': product['base_price'],
                    'Old Price': product['old_price'],
                    'Min Price': product['min_price'],
                    'Currency': product['currency_code'],
                    'Offer ID': product.get('offer_id', '')
                })
            
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8')
            messagebox.showinfo("Успех", f"Данные экспортированы в {filename}")
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка экспорта: {str(e)}")
    
    def __del__(self):
        """Корректное закрытие приложения"""
        self.cleanup()
        
    def cleanup(self):
        """Очистка ресурсов"""
        try:
            if self.session and not self.session.closed:
                self.loop.run_until_complete(self.session.close())
            if self.loop and not self.loop.is_closed():
                self.loop.close()
        except:
            pass


def main():
    """Главная функция приложения"""
    root = tk.Tk()
    app = OzonProductManager(root)
    
    try:
        root.mainloop()
    except KeyboardInterrupt:
        pass
    finally:
        # Корректное закрытие ресурсов
        app.cleanup()


if __name__ == "__main__":
    main()