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
import subprocess
import threading
import time
from typing import Dict, List, Any



def setup_python_environment():
    """Настройка окружения для использования Python 3.13"""
    import sys
    import subprocess
    
    # Проверяем текущую версию Python
    current_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    logger.info(f"Текущая версия Python: {current_version}")
    
    if current_version != "3.13":
        logger.warning(f"Обнаружена версия Python {current_version}, требуется 3.13")
        
        # Пытаемся найти Python 3.13 в системе
        try:
            py313_path = subprocess.check_output(["where", "python3.13"], text=True).strip()
            if py313_path:
                logger.info(f"Найден Python 3.13 по пути: {py313_path}")
                return py313_path
        except subprocess.CalledProcessError:
            pass
        
        logger.warning("Не удалось автоматически найти Python 3.13. Требуется ручная настройка.")
    
    return sys.executable


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
        if not self.product_manager:
            return {}, []
            
        all_actions = await self.product_manager.get_actions()
        active_titles = [action.get('title', '') for action in self.product_info.get('marketing_actions', [])]
        
        return all_actions, active_titles
    
    def load_actions(self):
        """Загрузка информации об акциях"""
        if not self.product_manager:
            return
            
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
            if not self.product_manager:
                return []
                
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
            if not self.product_manager:
                return []
                
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
        if self.product_manager:
            self.product_manager.clear_actions_cache()
            self.load_actions()
    
    def apply_changes(self):
        """Применение изменений"""
        if not self.product_manager:
            messagebox.showerror("Ошибка", "Менеджер товаров не инициализирован")
            return
            
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


class DataTableManager:
    """Менеджер для работы с таблицей данных"""
    
    def __init__(self, tree_widget, status_var):
        self.tree = tree_widget
        self.status_var = status_var
        self.data = pd.DataFrame()
        self.filtered_data = pd.DataFrame()
        self.visible_columns = {
            "Название товара": True,
            "base_price": True,
            "old_price": True,
            "marketing_price": True,
            "min_price": True,
            "Цена": True
        }
        self.all_columns = []
        self.checkbox_vars = {}  # Инициализация пустого словаря
        
    def load_csv_data(self, filepath="out/data.csv"):
        """Загрузка данных из CSV файла"""
        try:
            if not os.path.exists(filepath):
                self.status_var.set("Файл data.csv не найден")
                return False
                
            self.data = pd.read_csv(filepath, encoding='utf-8')
            
            # Создаем список всех колонок
            self.all_columns = list(self.data.columns)
            
            # Обновляем видимые колонки
            for col in self.all_columns:
                if col not in self.visible_columns:
                    self.visible_columns[col] = False
            
            self.filtered_data = self.data.copy()
            self.update_table()
            self.status_var.set(f"Загружено {len(self.data)} записей")
            return True
            
        except Exception as e:
            self.status_var.set(f"Ошибка загрузки: {str(e)}")
            return False
    
    def update_table(self):
        """Обновление таблицы с данными"""
        # Очищаем таблицу
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Настраиваем колонки
        visible_cols = [col for col, visible in self.visible_columns.items() if visible and col in self.filtered_data.columns]
        
        # Устанавливаем колонки
        self.tree["columns"] = ["check"] + visible_cols
        self.tree["show"] = "headings"
        
        # Настраиваем заголовки
        self.tree.heading("check", text="✓")
        self.tree.column("check", width=30, stretch=False)
        
        for col in visible_cols:
            display_name = self.get_display_name(col)
            self.tree.heading(col, text=display_name)
            self.tree.column(col, width=100, stretch=True)
        
        # Заполняем данными
        self.checkbox_vars = {}
        for idx, row in self.filtered_data.iterrows():
            values = [""] + [str(row[col]) if col in row else "" for col in visible_cols]
            item = self.tree.insert("", "end", values=values)
            self.checkbox_vars[item] = tk.BooleanVar(value=False)
    
    def get_display_name(self, column_name):
        """Получение отображаемого имени колонки"""
        display_names = {
            "Название товара": "Название товара",
            "base_price": "Базовая цена API",
            "old_price": "Старая цена API",
            "marketing_price": "Маркетинговая цена API",
            "min_price": "Минимальная цена API",
            "Цена": "Цена 1С"
        }
        return display_names.get(column_name, column_name)
    
    def filter_data(self, filters: Dict[str, str]):
        """Фильтрация данных"""
        if self.data.empty:
            return
            
        self.filtered_data = self.data.copy()
        
        for column, value in filters.items():
            if column in self.filtered_data.columns and value:
                self.filtered_data = self.filtered_data[
                    self.filtered_data[column].astype(str).str.contains(value, case=False, na=False)
                ]
        
        self.update_table()
        self.status_var.set(f"Отфильтровано {len(self.filtered_data)} записей")
    
    def toggle_column_visibility(self, column: str, visible: bool):
        """Переключение видимости колонки"""
        if column in self.visible_columns:
            self.visible_columns[column] = visible
            self.update_table()
    
    def get_selected_products(self):
        """Получение выбранных товаров"""
        if not hasattr(self, 'checkbox_vars') or not self.checkbox_vars:
            return []
            
        selected = []
        for item, var in self.checkbox_vars.items():
            if var.get():
                # Получаем индекс строки в отфильтрованных данных
                item_index = self.tree.index(item)
                if item_index < len(self.filtered_data):
                    product_data = self.filtered_data.iloc[item_index].to_dict()
                    selected.append(product_data)
        
        return selected
    
    def select_filtered(self, selected: bool = True):
        """Выделение всех отфильтрованных товаров"""
        if not hasattr(self, 'checkbox_vars') or not self.checkbox_vars:
            return
            
        for item in self.checkbox_vars:
            self.checkbox_vars[item].set(selected)


class ExternalModuleManager:
    """Менеджер для работы с внешними модулями"""
    def __init__(self):
        # Автоматическое определение пути к Python 3.13
        self.python_path = self.find_python_3_13()
        logger.info(f"Используется Python: {self.python_path}")
    
    def find_python_3_13(self):
        """Поиск пути к Python 3.13"""
        # Проверяем стандартные пути установки
        possible_paths = [
            "python3.13",  # Unix-like системы
            "py -3.13",    # Windows с Python Launcher
            "C:\\Python313\\python.exe",  # Windows стандартная установка
            "C:\\Users\\User\\AppData\\Local\\Programs\\Python\\Python313\\python.exe",  # Windows пользовательская установка
            "/usr/bin/python3.13",  # Linux/Mac стандартный путь
            "/usr/local/bin/python3.13"  # Homebrew на Mac
        ]
        
        for path in possible_paths:
            try:
                # Проверяем, существует ли такой интерпретатор и какая у него версия
                result = subprocess.run([path, "--version"], 
                                      capture_output=True, text=True, timeout=5)
                if result.returncode == 0 and "3.13" in result.stdout.lower():
                    return path
            except (FileNotFoundError, subprocess.TimeoutExpired):
                continue
        
        # Если не нашли конкретный путь, пытаемся использовать системный python с указанием версии
        try:
            result = subprocess.run(["python", "--version"], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and "3.13" in result.stdout.lower():
                return "python"
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        
        # Если ничего не сработало, используем общий путь, но предупреждаем пользователя
        logger.warning("Не удалось найти Python 3.13. Используется стандартный путь.")
        return "python"
        
    def run_get_data_module(self):
        """Запуск модуля get_data-api.py с увеличенным таймаутом"""
        try:
            logger.info(f"Запуск get_data-api.py с помощью {self.python_path}")
            # Увеличиваем таймаут до 600 секунд (10 минут)
            result = subprocess.run(
                [self.python_path, "get_data-api.py"], 
                capture_output=True, 
                text=True, 
                encoding='utf-8',
                timeout=600  # 10 минут на выполнение
            )
            logger.info(f"Модуль завершил работу с кодом {result.returncode}")
            if result.stdout:
                logger.debug(f"Вывод модуля: {result.stdout[:500]}...")  # Логируем только начало вывода
            if result.stderr:
                logger.warning(f"Ошибки модуля: {result.stderr}")
            return result.returncode == 0, result.stdout, result.stderr
        except subprocess.TimeoutExpired as e:
            error_msg = f"Модуль get_data-api.py выполнялся дольше 10 минут и был прерван"
            logger.error(error_msg)
            if hasattr(e, 'stdout') and e.stdout:
                logger.debug(f"Частичный вывод перед таймаутом: {e.stdout.decode()[:500]}...")
            return False, "", error_msg
        except FileNotFoundError:
            error_msg = f"Не найден исполняемый файл Python по пути: {self.python_path}. Проверьте установку Python 3.13."
            logger.error(error_msg)
            return False, "", error_msg
        except Exception as e:
            error_msg = f"Ошибка при запуске модуля: {str(e)}"
            logger.error(error_msg)
            return False, "", error_msg
    
    @staticmethod
    def create_inter_check_file(products_data):
        """Создание файла in/inter_check_up.txt"""
        try:
            os.makedirs("in", exist_ok=True)
            with open("in/inter_check_up.txt", "w", encoding="utf-8") as f:
                for product in products_data:
                    product_id = product.get("Ozon Product ID", "")
                    # Здесь нужно получить ссылку на товар, предположим что она формируется по шаблону
                    link = f"https://www.ozon.ru/product/{product_id}/"
                    f.write(f"{link}\t{product_id}\n")
            return True
        except Exception as e:
            return False, str(e)
    
    @staticmethod
    def run_pars_link_module():
        """Запуск модуля pars_link.py"""
        try:
            result = subprocess.run(["python", "pars_link.py"], 
                                  capture_output=True, text=True, encoding='utf-8')
            return result.returncode == 0, result.stdout, result.stderr
        except Exception as e:
            return False, "", str(e)


class OzonProductManager:
    """Главное приложение для управления товарами Ozon"""
    def __init__(self, root):
        self.root = root
        self.root.title("Ozon Product Manager - Расширенная версия")
        self.root.geometry("1400x800")
        
        # Инициализация асинхронного цикла
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Создаем status_var ДО создания UI
        self.status_var = tk.StringVar()
        self.status_var.set("Готов к работе")
        
        # Инициализация API и менеджеров
        self.session = None
        self.api = None
        self.product_manager = None
        self.data_table_manager = None
        
        # Инициализация ExternalModuleManager (только один раз)
        self.external_module_manager = ExternalModuleManager()
        
        # Данные приложения
        self.products = {}
        self.current_filters = {}
        
        # Создаем UI после инициализации всех необходимых атрибутов
        self.create_ui()
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
        # Главный контейнер
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Верхняя панель управления
        self.create_control_panel(main_frame)
        
        # Панель фильтров
        self.create_filter_panel(main_frame)
        
        # Таблица данных
        self.create_data_table(main_frame)
        
        # Панель действий с выбранными товарами
        self.create_action_panel(main_frame)
        
        # Статус бар
        self.create_status_bar(main_frame)
    
    def create_control_panel(self, parent):
        """Создание панели управления"""
        control_frame = ttk.LabelFrame(parent, text="Управление данными", padding="10")
        control_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Первая строка кнопок
        row1 = ttk.Frame(control_frame)
        row1.pack(fill=tk.X)
        
        ttk.Button(row1, text="Запуск get_data-api.py", 
                  command=self.run_get_data_module).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(row1, text="Загрузить данные из CSV", 
                  command=self.load_csv_data).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(row1, text="Экспорт в Excel", 
                  command=self.export_to_excel).pack(side=tk.LEFT, padx=5)
        
        # Вторая строка - управление колонками
        row2 = ttk.Frame(control_frame)
        row2.pack(fill=tk.X, pady=(5, 0))
        
        ttk.Label(row2, text="Колонки:").pack(side=tk.LEFT)
        
        # Выпадающее меню для управления колонками
        self.column_menu = ttk.Menubutton(row2, text="Управление колонками")
        self.column_menu.pack(side=tk.LEFT, padx=5)
        self.create_column_menu()
    
    def create_filter_panel(self, parent):
        """Создание панели фильтров"""
        filter_frame = ttk.LabelFrame(parent, text="Фильтры", padding="10")
        filter_frame.pack(fill=tk.X, pady=(0, 10))
        
        filter_container = ttk.Frame(filter_frame)
        filter_container.pack(fill=tk.X)
        
        # Основные поля для фильтрации
        main_filters = ["Название товара", "base_price", "old_price", "marketing_price", "min_price", "Цена"]
        
        for i, filter_name in enumerate(main_filters):
            ttk.Label(filter_container, text=f"{filter_name}:").grid(row=0, column=i*2, padx=5, pady=2, sticky='w')
            entry = ttk.Entry(filter_container, width=15)
            entry.grid(row=0, column=i*2+1, padx=5, pady=2)
            entry.bind('<KeyRelease>', lambda e, fn=filter_name: self.apply_filter(fn, e.widget.get()))
        
        # Кнопки управления фильтрами
        filter_buttons = ttk.Frame(filter_frame)
        filter_buttons.pack(fill=tk.X, pady=(5, 0))
        
        ttk.Button(filter_buttons, text="Применить фильтры", 
                  command=self.apply_all_filters).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(filter_buttons, text="Сбросить фильтры", 
                  command=self.reset_filters).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(filter_buttons, text="Выделить отфильтрованные", 
                  command=self.select_filtered_items).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(filter_buttons, text="Снять выделение", 
                  command=self.deselect_all_items).pack(side=tk.LEFT, padx=5)
    
    def create_data_table(self, parent):
        """Создание таблицы данных"""
        table_frame = ttk.LabelFrame(parent, text="Данные товаров", padding="10")
        table_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # Создаем Treeview с прокруткой
        tree_frame = ttk.Frame(table_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        self.data_tree = ttk.Treeview(tree_frame, show="headings")
        
        # Прокрутки
        v_scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.data_tree.yview)
        h_scrollbar = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.data_tree.xview)
        self.data_tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        self.data_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Инициализируем менеджер таблицы
        self.data_table_manager = DataTableManager(self.data_tree, self.status_var)
        
        # Привязываем обработчик кликов по чекбоксам
        self.data_tree.bind('<Button-1>', self.on_tree_click)
    
    def create_action_panel(self, parent):
        """Создание панели действий"""
        self.action_frame = ttk.LabelFrame(parent, text="Действия с выбранными товарами", padding="10")
        self.action_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Сначала скрываем панель
        self.action_frame.pack_forget()
        
        action_buttons = ttk.Frame(self.action_frame)
        action_buttons.pack(fill=tk.X)
        
        ttk.Button(action_buttons, text="Начать корректировку цен", 
                  command=self.start_price_correction).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(action_buttons, text="Обновить данные о ценах по карте озон", 
                  command=self.update_ozon_card_prices).pack(side=tk.LEFT, padx=5)
    
    def create_status_bar(self, parent):
        """Создание статус бара"""
        status_frame = ttk.Frame(parent)
        status_frame.pack(fill=tk.X, side=tk.BOTTOM)
        # Используем уже существующий status_var, а не создаем новый
        status_bar = ttk.Label(status_frame, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)
    
    def create_column_menu(self):
        """Создание меню управления колонками"""
        menu = tk.Menu(self.column_menu, tearoff=0)
        self.column_menu["menu"] = menu
        
        # Будет заполнено после загрузки данных
        self.column_menu_menu = menu
    
    def update_column_menu(self):
        """Обновление меню колонок"""
        if not hasattr(self, 'column_menu_menu') or not self.data_table_manager:
            return
            
        menu = self.column_menu_menu
        menu.delete(0, 'end')
        
        for column in self.data_table_manager.all_columns:
            var = tk.BooleanVar(value=self.data_table_manager.visible_columns.get(column, False))
            menu.add_checkbutton(
                label=column,
                variable=var,
                command=lambda c=column, v=var: self.toggle_column_visibility(c, v.get())
            )
    
    def toggle_column_visibility(self, column: str, visible: bool):
        """Переключение видимости колонки"""
        if self.data_table_manager:
            self.data_table_manager.toggle_column_visibility(column, visible)
            self.check_selection_state()
    
    def run_get_data_module(self):
        """Запуск внешнего модуля get_data-api.py с обработкой ошибок и повторными попытками"""
        if not hasattr(self, 'status_var'):
            return
            
        self.status_var.set("Запуск get_data-api.py...")
        # Обновляем интерфейс немедленно
        self.root.update_idletasks()
        
        def run_module():
            max_attempts = 3
            attempt = 1
            success = False
            stdout = ""
            stderr = ""
            
            while attempt <= max_attempts and not success:
                if attempt > 1:
                    self.root.after(0, lambda a=attempt: 
                        self.status_var.set(f"Попытка {a} из {max_attempts}..."))
                    self.root.update_idletasks()
                    time.sleep(10)  # Увеличенная задержка между попытками
                    
                logger.info(f"Попытка запуска get_data-api.py #{attempt}")
                success, stdout, stderr = self.external_module_manager.run_get_data_module()
                
                if not success:
                    logger.warning(f"Попытка {attempt} завершилась с ошибкой: {stderr}")
                
                attempt += 1
            
            def update_ui():
                if success:
                    if hasattr(self, 'status_var'):
                        self.status_var.set("Модуль успешно завершен. Ожидание обновления данных...")
                    
                    # Добавляем задержку, чтобы файл успел полностью записаться
                    time.sleep(2)  # Задержка 2 секунды
                    
                    # Проверяем, какой файл создал модуль
                    data_file = "out/data.csv"
                    if not os.path.exists(data_file):
                        # Проверяем альтернативные пути
                        alt_files = [
                            "in/products_update_full.xlsx",
                            "out/products_update_full.xlsx",
                            "products_update_full.xlsx"
                        ]
                        for alt_file in alt_files:
                            if os.path.exists(alt_file):
                                data_file = alt_file
                                break
                    
                    # Если файл в формате Excel, конвертируем его в CSV
                    if data_file.endswith('.xlsx') and os.path.exists(data_file):
                        try:
                            df = pd.read_excel(data_file)
                            # Сохраняем в правильном формате
                            csv_path = "out/data.csv"
                            os.makedirs("out", exist_ok=True)
                            df.to_csv(csv_path, index=False, encoding='utf-8')
                            logger.info(f"Файл {data_file} успешно конвертирован в {csv_path}")
                        except Exception as e:
                            logger.error(f"Ошибка конвертации Excel в CSV: {str(e)}")
                            if hasattr(self, 'status_var'):
                                self.status_var.set(f"Ошибка конвертации данных: {str(e)}")
                            messagebox.showerror("Ошибка", f"Не удалось конвертировать данные: {str(e)}")
                            return
                    
                    # Теперь загружаем данные из правильного файла
                    if self.data_table_manager:
                        success_load = self.data_table_manager.load_csv_data()
                        if success_load:
                            self.status_var.set("Данные успешно обновлены и отображены")
                            self.update_column_menu()
                            self.check_selection_state()
                            messagebox.showinfo("Успех", "Данные успешно обновлены!")
                        else:
                            error_msg = "Не удалось загрузить данные после выполнения модуля. Проверьте наличие файла out/data.csv"
                            self.status_var.set(error_msg)
                            logger.error(error_msg)
                            messagebox.showwarning("Предупреждение", error_msg)
                else:
                    error_msg = f"Не удалось выполнить модуль после {max_attempts} попыток:\n{stderr}"
                    if hasattr(self, 'status_var'):
                        self.status_var.set("Ошибка выполнения модуля")
                    logger.error(error_msg)
                    # Показываем подробную ошибку
                    messagebox.showerror("Ошибка выполнения", 
                                        f"Не удалось выполнить get_data-api.py после {max_attempts} попыток.\n\n"
                                        f"Детали ошибки:\n{stderr[:500]}{'...' if len(stderr) > 500 else ''}\n\n"
                                        f"Проверьте:\n"
                                        f"1. Установлен ли Python 3.13\n"
                                        f"2. Доступность API Ozon\n"
                                        f"3. Корректность конфигурационных файлов")
            
            self.root.after(0, update_ui)
        
        # Запускаем в отдельном потоке, чтобы не блокировать UI
        thread = threading.Thread(target=run_module, daemon=True)
        thread.start()
    
    
    def copy_to_clipboard(self, text):
        """Копирование текста в буфер обмена"""
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        messagebox.showinfo("Успех", "Текст ошибки скопирован в буфер обмена")
        
        
    
    def load_csv_data(self):
        """Загрузка данных из CSV"""
        if not self.data_table_manager:
            messagebox.showerror("Ошибка", "Менеджер данных не инициализирован")
            return
            
        if self.data_table_manager.load_csv_data():
            self.update_column_menu()
            self.check_selection_state()
    
    def apply_filter(self, column: str, value: str):
        """Применение фильтра"""
        self.current_filters[column] = value
    
    def apply_all_filters(self):
        """Применение всех фильтров"""
        if self.data_table_manager:
            self.data_table_manager.filter_data(self.current_filters)
            self.check_selection_state()
    
    def reset_filters(self):
        """Сброс фильтров"""
        self.current_filters = {}
        if self.data_table_manager:
            self.data_table_manager.filter_data({})
            self.check_selection_state()
    
    def select_filtered_items(self):
        """Выделение всех отфильтрованных товаров"""
        if self.data_table_manager:
            self.data_table_manager.select_filtered(True)
            self.check_selection_state()
    
    def deselect_all_items(self):
        """Снятие выделения со всех товаров"""
        if self.data_table_manager:
            self.data_table_manager.select_filtered(False)
            self.check_selection_state()
    
    def on_tree_click(self, event):
        """Обработчик кликов по таблице"""
        if not self.data_table_manager:
            return
            
        item = self.data_tree.identify_row(event.y)
        column = self.data_tree.identify_column(event.x)
        
        if item and column == "#1":  # Колонка с чекбоксами
            current_state = self.data_table_manager.checkbox_vars[item].get()
            self.data_table_manager.checkbox_vars[item].set(not current_state)
            self.check_selection_state()
    
    def check_selection_state(self):
        """Проверка состояния выделения и отображение панели действий"""
        if not self.data_table_manager:
            return
            
        selected_products = self.data_table_manager.get_selected_products()
        has_selection = len(selected_products) > 0
        
        if has_selection:
            self.action_frame.pack(fill=tk.X, pady=(0, 10))
            self.status_var.set(f"Выбрано товаров: {len(selected_products)}")
        else:
            self.action_frame.pack_forget()
    
    def start_price_correction(self):
        """Начало корректировки цен"""
        if not self.data_table_manager:
            messagebox.showerror("Ошибка", "Менеджер данных не инициализирован")
            return
            
        selected_products = self.data_table_manager.get_selected_products()
        messagebox.showinfo("Информация", f"Запуск корректировки цен для {len(selected_products)} товаров")
        # Здесь будет логика корректировки цен
        # pass
    
    def update_ozon_card_prices(self):
        """Обновление цен по карте Ozon"""
        if not self.data_table_manager:
            messagebox.showerror("Ошибка", "Менеджер данных не инициализирован")
            return
            
        selected_products = self.data_table_manager.get_selected_products()

        if not selected_products:
            messagebox.showwarning("Предупреждение", "Не выбраны товары для обновления")
            return
        
        # Создаем файл со ссылками
        success = self.external_module_manager.create_inter_check_file(selected_products)
        
        if not success:
            messagebox.showerror("Ошибка", "Не удалось создать файл со ссылками")
            return
        
        # Проверка наличия status_var
        if hasattr(self, 'status_var'):
            self.status_var.set("Запуск обновления цен по карте Ozon...")
        
        def run_pars_module():
            success, stdout, stderr = self.external_module_manager.run_pars_link_module()
            
            def update_ui():
                if hasattr(self, 'status_var'):
                    if success:
                        self.status_var.set("Цены по карте Ozon обновлены")
                        # Перезагружаем данные
                        self.load_csv_data()
                    else:
                        self.status_var.set(f"Ошибка обновления цен: {stderr}")
                        messagebox.showerror("Ошибка", f"Ошибка обновления цен:\n{stderr}")
            
            self.root.after(0, update_ui)
        
        # Запускаем в отдельном потоке
        thread = threading.Thread(target=run_pars_module)
        thread.daemon = True
        thread.start()
    
    def export_to_excel(self):
        """Экспорт данных в Excel"""
        if not self.data_table_manager or self.data_table_manager.data.empty:
            messagebox.showwarning("Предупреждение", "Нет данных для экспорта")
            return
        
        filename = filedialog.asksaveasfilename(
            title="Экспорт в Excel",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        
        if not filename:
            return
        
        try:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            export_filename = f"out/result_price_{timestamp}.xlsx"
            
            # Сохраняем в указанную папку
            self.data_table_manager.filtered_data.to_excel(export_filename, index=False)
            messagebox.showinfo("Успех", f"Данные экспортированы в {export_filename}")
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка экспорта: {str(e)}")
    
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