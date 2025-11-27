# correct_price.py


import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import asyncio
import aiohttp
import random
from datetime import datetime
from loguru import logger
from conf import BASE_URL, CLIENT_ID, API_KEY

class Config:
    BASE_URL = BASE_URL
    CLIENT_ID = CLIENT_ID
    API_KEY = API_KEY
    REQUEST_TIMEOUT = 30
    PRODUCT_DELAY_RANGE = (4, 15)
conf = Config()

class ActionManagerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Управление акциями и ценами товаров")
        self.root.geometry("1200x700")
        
        # Инициализация асинхронного цикла
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Инициализация переменных
        self.session = None
        self.current_product_id = None
        self.cached_actions = None
        self.current_prices = {}
        self.current_marketing_actions = []
        
        # Создание интерфейса
        self.create_ui()
        
        # Инициализация сессии
        self.loop.run_until_complete(self.init_session())

    async def init_session(self):
        """Инициализация асинхронной сессии"""
        if self.session and not self.session.closed:
            return

        headers = {
            'Client-Id': conf.CLIENT_ID,
            'Api-Key': conf.API_KEY,
            'Content-Type': 'application/json'
        }
        timeout = aiohttp.ClientTimeout(total=conf.REQUEST_TIMEOUT)
        self.session = aiohttp.ClientSession(headers=headers, timeout=timeout)
        logger.info("Сессия успешно создана")

    def create_ui(self):
        """Создание пользовательского интерфейса"""
        # Поля ввода
        input_frame = ttk.Frame(self.root, padding="10")
        input_frame.pack(fill=tk.X)
        
        ttk.Label(input_frame, text="Product ID:").pack(side=tk.LEFT)
        self.product_id_entry = ttk.Entry(input_frame, width=20)
        self.product_id_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(input_frame, text="Загрузить данные", command=self.load_all_data).pack(side=tk.LEFT, padx=5)
        
        # Вкладки
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Вкладка акций
        self.create_actions_tab()
        
        # Вкладка цен
        self.create_prices_tab()
        
        # Вкладка логов
        self.create_log_tab()

    def create_actions_tab(self):
        """Создание вкладки управления акциями"""
        actions_frame = ttk.Frame(self.notebook)
        self.notebook.add(actions_frame, text="Акции")
        
        # Notebook для подвкладок
        sub_notebook = ttk.Notebook(actions_frame)
        sub_notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Участвует в акциях
        active_frame = ttk.Frame(sub_notebook)
        sub_notebook.add(active_frame, text="Участвует в акциях")
        
        self.active_tree = ttk.Treeview(active_frame, columns=("Action"), show="headings")
        self.active_tree.heading("Action", text="Акция")
        self.active_tree.column("Action", width=800)
        self.active_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Доступные акции
        available_frame = ttk.Frame(sub_notebook)
        sub_notebook.add(available_frame, text="Доступные акции")
        
        self.available_tree = ttk.Treeview(available_frame, columns=("Action"), show="headings")
        self.available_tree.heading("Action", text="Акция")
        self.available_tree.column("Action", width=800)
        self.available_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Управление
        control_frame = ttk.Frame(actions_frame)
        control_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(control_frame, text="Деактивировать акцию", command=self.deactivate_selected).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Активировать акцию", command=self.activate_selected).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Обновить список", command=self.refresh_actions).pack(side=tk.LEFT, padx=5)

    def create_prices_tab(self):
        """Создание вкладки управления ценами"""
        prices_frame = ttk.Frame(self.notebook)
        self.notebook.add(prices_frame, text="Цены")
        
        # Таблица цен
        self.prices_tree = ttk.Treeview(prices_frame, columns=("Parameter", "Value"), show="headings")
        self.prices_tree.heading("Parameter", text="Параметр")
        self.prices_tree.heading("Value", text="Значение")
        self.prices_tree.column("Parameter", width=300)
        self.prices_tree.column("Value", width=400)
        self.prices_tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Кнопки управления
        control_frame = ttk.Frame(prices_frame)
        control_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(control_frame, text="Получить данные", command=self.get_prices).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Обновить цену", command=self.update_selected_price).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Обновить все цены", command=self.update_all_prices).pack(side=tk.LEFT, padx=5)

    def create_log_tab(self):
        """Создание вкладки логов"""
        log_frame = ttk.Frame(self.notebook)
        self.notebook.add(log_frame, text="Лог")
        
        self.log_text = tk.Text(log_frame, wrap=tk.NONE, state='disabled')
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Прокрутка
        xscroll = ttk.Scrollbar(log_frame, orient='horizontal', command=self.log_text.xview)
        xscroll.pack(side=tk.BOTTOM, fill=tk.X)
        self.log_text['xscrollcommand'] = xscroll.set
        
        yscroll = ttk.Scrollbar(log_frame, orient='vertical', command=self.log_text.yview)
        yscroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text['yscrollcommand'] = yscroll.set

    def load_all_data(self):
        """Загрузка всех данных по товару"""
        try:
            self.current_product_id = int(self.product_id_entry.get())
            self.loop.run_until_complete(self._get_product_info())
            self.loop.run_until_complete(self._load_actions(self.current_product_id))
        except ValueError:
            messagebox.showerror("Ошибка", "Введите корректный Product ID")
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
            messagebox.showerror("Ошибка", f"Произошла ошибка: {str(e)}")

    async def _get_product_info(self):
        """Асинхронное получение информации о товаре через /v3/product/info/list"""
        if not self.current_product_id:
            logger.error("Product ID не задан")
            return

        payload = {"product_id": [str(self.current_product_id)]}
        data = await self.api_request('POST', '/v3/product/info/list', payload)

        if data and 'items' in data and data['items']:
            item = data['items'][0]
            price_info = {
                'price': item.get('price', '0'),
                'old_price': item.get('old_price', '0'),
                'min_price': item.get('min_price', '0'),
                'currency_code': item.get('currency_code', 'RUB'),
                'offer_id': item.get('offer_id', '')
            }
            
            # Получаем маркетинговые акции корректно
            marketing_actions = []
            marketing_info = item.get('marketing_actions')
            if isinstance(marketing_info, dict):
                marketing_actions = marketing_info.get('actions', [])
            elif isinstance(marketing_info, list):
                marketing_actions = marketing_info

            # Сохраняем данные
            self.current_prices = price_info
            self.current_marketing_actions = marketing_actions

            # Обновляем таблицу цен
            self.prices_tree.delete(*self.prices_tree.get_children())
            for key, value in price_info.items():
                self.prices_tree.insert("", tk.END, values=(key, str(value)))

            # Обновляем список акций
            self.active_tree.delete(*self.active_tree.get_children())
            for action in marketing_actions:
                title = action.get('title', 'Без названия')
                self.active_tree.insert("", tk.END, values=(title,))

            logger.info("Информация о товаре успешно загружена")
        else:
            error_msg = data.get('error', 'Неизвестная ошибка') if data else 'Нет ответа от сервера'
            messagebox.showerror("Ошибка API", f"Не удалось получить данные: {error_msg}")
            logger.error(f"Ошибка при получении информации о товаре: {error_msg}")

    async def _load_actions(self, product_id):
        """Асинхронная загрузка информации об акциях"""
        if not self.session or self.session.closed:
            await self.init_session()
            
        if self.cached_actions is None:
            self.cached_actions = await self.get_actions()

        all_actions = self.cached_actions
        if not all_actions:
            messagebox.showwarning("Предупреждение", "Не удалось загрузить список акций")
            return

        # Проверяем, в каких акциях участвует товар
        payload = {"product_id": [str(product_id)]}
        data = await self.api_request('POST', '/v3/product/info/list', payload)

        active_titles = []
        if data and 'items' in data and data['items']:
            item = data['items'][0]
            marketing_actions = []
            marketing_info = item.get('marketing_actions')
            if isinstance(marketing_info, dict):
                marketing_actions = marketing_info.get('actions', [])
            elif isinstance(marketing_info, list):
                marketing_actions = marketing_info
            
            active_titles = [action.get('title', '') for action in marketing_actions]

        # Обновляем деревья
        self.active_tree.delete(*self.active_tree.get_children())
        for action in active_titles:
            self.active_tree.insert("", tk.END, values=(action,))

        self.available_tree.delete(*self.available_tree.get_children())
        for action in all_actions.keys():
            if action not in active_titles:
                self.available_tree.insert("", tk.END, values=(action,))

    async def get_actions(self):
        """Получение списка всех доступных акций"""
        if not self.session or self.session.closed:
            await self.init_session()

        data = await self.api_request('GET', '/v1/actions')
        actions = {}
        if data and 'result' in data:
            for act in data['result']:
                if 'title' in act and 'id' in act:
                    actions[act['title']] = act['id']
            logger.info(f"Получено {len(actions)} акций")
        return actions

    async def check_in_actions(self, product_id, actions):
        """Проверка участия товара в акциях"""
        found_titles = []
        for title, aid in actions.items():
            payload = {'action_id': aid, 'limit': 500}
            last_id = None
            attempts = 0

            while attempts < 3:
                if last_id:
                    payload['last_id'] = last_id

                data = await self.api_request('POST', '/v1/actions/products', payload)

                if not data:
                    break

                # Проверяем наличие 'result' перед доступом к нему
                if 'result' not in data:
                    continue

                products = data['result'].get('products', [])
                if any(p.get('id') == product_id for p in products):
                    found_titles.append(title)
                    logger.info(f"Товар {product_id} найден в акции '{title}'")
                    break

                last_id = data['result'].get('last_id')
                if not last_id:
                    break

                attempts += 1

        return found_titles

    def get_prices(self):
        """Получение информации о ценах товара"""
        if not self.current_product_id:
            messagebox.showwarning("Предупреждение", "Сначала введите Product ID")
            return
            
        self.loop.run_until_complete(self._get_product_info())

    def update_selected_price(self):
        """Обновление выбранной цены"""
        selection = self.prices_tree.selection()
        if not selection:
            messagebox.showwarning("Предупреждение", "Выберите цену для изменения")
            return

        param, old_value = self.prices_tree.item(selection[0])['values']
        new_value = simpledialog.askstring(
            "Изменение цены",
            f"Введите новое значение для {param}:\nТекущее значение: {old_value}",
            initialvalue=old_value
        )

        if new_value is not None and new_value != old_value:
            self.loop.run_until_complete(self._update_price(param, new_value))

    def update_all_prices(self):
        """Обновление всех цен"""
        dialog = PriceUpdateDialog(self.root, "Обновление всех цен", self.current_prices)
        if dialog.result:
            self.loop.run_until_complete(self._update_all_prices(dialog.result))

    async def _update_all_prices(self, new_prices):
        """Асинхронное обновление всех цен"""
        offer_id = self.current_prices.get('offer_id')
        if not offer_id:
            messagebox.showerror("Ошибка", "Не найден offer_id для товара")
            return
            
        payload = {
            'prices': [{
                'offer_id': offer_id,
                'old_price': str(new_prices.get('old_price', 0)),
                'price': str(new_prices.get('price', 0)),
                'min_price': str(new_prices.get('min_price', 0)),
                'currency_code': new_prices.get('currency_code', 'RUB'),
                'auto_action_enabled': 'UNKNOWN',
                'auto_add_to_ozon_actions_list_enabled': 'UNKNOWN',
                'price_strategy_enabled': 'DISABLED',
                'min_price_for_auto_actions_enabled': True
            }]
        }
        
        if await self.api_request('POST', '/v1/product/import/prices', payload):
            messagebox.showinfo("Успех", "Все цены успешно обновлены")
            await self._get_product_info()

    async def _update_price(self, param, new_value):
        """Асинхронное обновление конкретной цены"""
        offer_id = self.current_prices.get('offer_id')
        if not offer_id:
            messagebox.showerror("Ошибка", "Не найден offer_id для товара")
            return

        payload = {
            'prices': [{
                'offer_id': offer_id,
                'currency_code': self.current_prices.get('currency_code', 'RUB'),
                'auto_action_enabled': 'UNKNOWN',
                'auto_add_to_ozon_actions_list_enabled': 'UNKNOWN',
                'price_strategy_enabled': 'DISABLED',
                'min_price_for_auto_actions_enabled': True
            }]
        }

        # Обновляем только изменённое значение
        payload['prices'][0][param] = str(new_value)

        # Восстанавливаем остальные значения из текущего состояния
        for key in ['old_price', 'price', 'min_price']:
            if key in payload['prices'][0]:
                continue
            payload['prices'][0][key] = str(self.current_prices.get(key, 0))

        if await self.api_request('POST', '/v1/product/import/prices', payload):
            # Обновляем текущее состояние цен
            self.current_prices[param] = new_value
            messagebox.showinfo("Успех", f"Цена '{param}' успешно обновлена")
            self.prices_tree.delete(*self.prices_tree.get_children())
            for key, value in self.current_prices.items():
                self.prices_tree.insert("", tk.END, values=(key, str(value)))
            
            
    async def api_request(self, method, endpoint, json_payload=None):
        """Асинхронный запрос к API с обработкой ошибок"""
        if not self.session or self.session.closed:
            await self.init_session()

        url = f"{conf.BASE_URL}{endpoint}"
        start_time = datetime.now()
        logger.debug(f"API-запрос к {endpoint} начат")

        for attempt in range(1, 6):
            try:
                async with self.session.request(method, url, json=json_payload) as resp:
                    text = await resp.text()
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.debug(f"API-запрос к {endpoint} завершен за {elapsed:.2f} секунд")

                    if resp.status == 200:
                        try:
                            result = await resp.json()
                            logger.debug(f"Получен ответ: {result}")
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

    def deactivate_selected(self):
        """Деактивация выбранной акции"""
        selection = self.active_tree.selection()
        if not selection:
            messagebox.showwarning("Предупреждение", "Выберите акцию для деактивации")
            return
            
        selected_action = self.active_tree.item(selection[0])['values'][0]
        product_id = self.current_product_id

        if not product_id:
            messagebox.showwarning("Предупреждение", "Сначала загрузите данные для товара")
            return
            
        self.loop.run_until_complete(self._deactivate_actions(product_id, [selected_action]))

    async def _deactivate_actions(self, product_id, titles):
        """Асинхронная деактивация акций"""
        if not self.cached_actions:
            self.cached_actions = await self.get_actions()
            
        result = await self.deactivate_actions(product_id, self.cached_actions, titles)
        if result:
            messagebox.showinfo("Успех", f"Акция '{result[0]}' деактивирована")
            await self._load_actions(product_id)
        else:
            messagebox.showerror("Ошибка", "Не удалось деактивировать акцию")

    async def deactivate_actions(self, product_id, actions, titles):
        """Функция деактивации акций"""
        if not self.session or self.session.closed:
            await self.init_session()

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

    def activate_selected(self):
        """Активация выбранной акции с пользовательским вводом цены"""
        selection = self.available_tree.selection()
        if not selection:
            messagebox.showwarning("Предупреждение", "Выберите акцию для активации")
            return

        selected_action = self.available_tree.item(selection[0])['values'][0]
        product_id = self.current_product_id

        if not product_id:
            messagebox.showwarning("Предупреждение", "Сначала загрузите данные для товара")
            return

        action_price = simpledialog.askinteger(
            "Цена акции",
            "Введите цену для активации в акции:",
            initialvalue=1000,
            minvalue=1,
            maxvalue=1000000
        )

        if action_price is None:
            messagebox.showwarning("Отмена", "Не указана цена для акции")
            return

        self.loop.run_until_complete(self._activate_actions(product_id, [selected_action], action_price))

    async def _activate_actions(self, product_id, titles, action_price):
        """Асинхронная активация акций"""
        if not self.cached_actions:
            self.cached_actions = await self.get_actions()
            
        result = await self.activate_actions(product_id, self.cached_actions, titles, action_price)
        if result:
            messagebox.showinfo("Успех", f"Акция '{result[0]}' активирована")
            await self._load_actions(product_id)
        else:
            messagebox.showerror("Ошибка", "Не удалось активировать акцию")

    async def activate_actions(self, product_id, actions, titles, action_price):
        """Функция активации акций"""
        if not self.session or self.session.closed:
            await self.init_session()

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

    def refresh_actions(self):
        """Обновление списка акций"""
        self.cached_actions = None
        if self.current_product_id:
            self.loop.run_until_complete(self._load_actions(self.current_product_id))

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
        
        for key, value in self.current_prices.items():
            ttk.Label(master, text=key).grid(row=row, column=0, padx=5, pady=5, sticky='w')
            entry = ttk.Entry(master, width=40)
            entry.grid(row=row, column=1, padx=5, pady=5)
            entry.insert(0, str(value))
            self.entries[key] = entry
            row += 1
            
        return self.entries[list(self.entries.keys())[0]]

    def apply(self):
        """Применение изменений"""
        self.result = {}
        for key, entry in self.entries.items():
            self.result[key] = entry.get()


def main():
    root = tk.Tk()
    app = ActionManagerApp(root)
    try:
        root.mainloop()
    finally:
        # Корректное закрытие сессии при выходе
        if app.session and not app.session.closed:
            app.loop.run_until_complete(app.session.close())
        app.loop.close()

if __name__ == "__main__":
    main()