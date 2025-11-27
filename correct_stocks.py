# correct_stocks.py


import tkinter as tk
from tkinter import ttk, messagebox
import asyncio
import aiohttp
import pickle
import os
import random
from datetime import datetime, timedelta, UTC
from conf import BASE_URL, CLIENT_ID, API_KEY
import loguru

# --- Configuration ---
class Config:
    BASE_URL = BASE_URL.rstrip('/')
    CLIENT_ID = CLIENT_ID
    API_KEY = API_KEY
    REQUEST_TIMEOUT = 30
    PRODUCT_DELAY_RANGE = (4, 15)
    FILE_CHECK_INTERVAL = 24 * 3600
conf = Config()

# --- Logger setup ---
logger = loguru.logger
logger.add("action_manager.log", rotation="10 MB", encoding="utf-8")

# --- API helpers with retry ---
async def api_request(session, method, endpoint, json_payload=None):
    url = f"{conf.BASE_URL}{endpoint}"
    start_time = datetime.now()
    logger.debug(f"API request to {endpoint} started")
    
    for attempt in range(1, 6):
        try:
            async with session.request(method, url, json=json_payload, ssl=False) as resp:
                text = await resp.text()
                elapsed = (datetime.now() - start_time).total_seconds()
                logger.debug(f"API request to {endpoint} completed in {elapsed:.2f}s")
                
                if resp.status == 200:
                    return await resp.json()
                    
                if resp.status in (429, 500, 502, 503, 504):
                    logger.warning(f"{endpoint} attempt {attempt} returned {resp.status}. Retrying...")
                    delay = 2 ** attempt + random.random()
                    logger.debug(f"Sleeping for {delay:.2f}s before retry")
                    await asyncio.sleep(delay)
                    continue
                    
                logger.error(f"API {endpoint} failed {resp.status}: {text}")
                return None
                
        except Exception as e:
            logger.warning(f"{endpoint} attempt {attempt} exception: {e}")
            delay = 2 ** attempt + random.random()
            await asyncio.sleep(delay)
            
    logger.error(f"API {endpoint} max retries exceeded")
    return None

# --- Action management ---
async def get_actions(session):
    data = await api_request(session, 'GET', '/v1/actions')
    actions = {}
    if data and 'result' in data:
        for act in data['result']:
            actions[act['title']] = act['id']
        logger.info(f"Fetched {len(actions)} actions")
    return actions

async def check_in_actions(session, product_id, actions):
    found_titles = []
    for title, aid in actions.items():
        payload = {'action_id': aid, 'limit': 500}
        last_id = None
        attempts = 0
        
        while attempts < 3:  # Лимит попыток
            if last_id:
                payload['last_id'] = last_id
                
            data = await api_request(session, 'POST', '/v1/actions/products', payload)
            
            if not data or 'result' not in data:
                break
                
            products = data['result'].get('products', [])
            
            if any(p.get('id') == product_id for p in products):
                found_titles.append(title)
                logger.info(f"Product {product_id} found in action '{title}'")
                break
                
            last_id = data['result'].get('last_id')
            if not last_id:
                break
                
            attempts += 1
            
    return found_titles

async def deactivate_actions(session, product_id, actions, titles):
    deactivated = []
    for title in titles:
        aid = actions.get(title)
        if not aid:
            continue
            
        logger.debug(f"Attempting deactivate {product_id} from '{title}'")
        payload = {'action_id': aid, 'product_ids': [product_id]}
        data = await api_request(session, 'POST', '/v1/actions/products/deactivate', payload)
        
        if data and product_id in data.get('result', {}).get('product_ids', []):
            deactivated.append(title)
            logger.info(f"Deactivated {product_id} from '{title}'")
            
    return deactivated

async def activate_actions(session, product_id, actions, titles, action_price):
    activated = []
    for title in titles:
        aid = actions.get(title)
        if not aid:
            continue
        
        logger.debug(f"Attempting activate {product_id} in '{title}' with price {action_price}")
        payload = {'action_id': aid, 'products': [{'product_id': product_id, 'action_price': action_price, 'stock': 10}]}
        data = await api_request(session, 'POST', '/v1/actions/products/activate', payload)
        
        if data and product_id in data.get('result', {}).get('product_ids', []):
            activated.append(title)
            logger.info(f"Re-activated {product_id} in '{title}'")
            
    return activated

class ActionManagerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Управление акциями товаров")
        self.root.geometry("1000x600")
        
        # Initialize async loop
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Create UI
        self.create_ui()
        
        # Initialize session
        self.session = None
        self.actions = {}
        self.current_product_id = None
        self.cached_actions = None  # Кэш акций
        
        # Setup async session
        self.loop.run_until_complete(self.init_session())

    async def init_session(self):
        headers = {
            'Client-Id': conf.CLIENT_ID,
            'Api-Key': conf.API_KEY,
            'Content-Type': 'application/json'
        }
        timeout = aiohttp.ClientTimeout(total=conf.REQUEST_TIMEOUT)
        self.session = aiohttp.ClientSession(headers=headers, timeout=timeout)

    def create_ui(self):
        input_frame = ttk.Frame(self.root, padding="10")
        input_frame.pack(fill=tk.X)

        ttk.Label(input_frame, text="Product ID:").pack(side=tk.LEFT)
        self.product_id_entry = ttk.Entry(input_frame, width=20)
        self.product_id_entry.pack(side=tk.LEFT, padx=5)

        ttk.Button(input_frame, text="Загрузить акции", command=self.load_actions).pack(side=tk.LEFT, padx=5)

        # Notebook для вкладок
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Вкладка: Участвует в акциях
        active_frame = ttk.Frame(self.notebook)
        self.notebook.add(active_frame, text="Участвует в акциях")

        self.active_tree = ttk.Treeview(active_frame, columns=("Action"), show="headings")
        self.active_tree.heading("Action", text="Акция")
        self.active_tree.column("Action", width=800)
        self.active_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Вкладка: Доступные акции
        available_frame = ttk.Frame(self.notebook)
        self.notebook.add(available_frame, text="Доступные акции")

        self.available_tree = ttk.Treeview(available_frame, columns=("Action"), show="headings")
        self.available_tree.heading("Action", text="Акция")
        self.available_tree.column("Action", width=800)
        self.available_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Управление
        control_frame = ttk.Frame(self.root, padding="10")
        control_frame.pack(fill=tk.X)

        ttk.Button(control_frame, text="Деактивировать выбранную акцию", command=self.deactivate_selected).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Активировать выбранную акцию", command=self.activate_selected).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Обновить список акций", command=self.refresh_actions).pack(side=tk.LEFT, padx=5)

    def load_actions(self):
        try:
            product_id = int(self.product_id_entry.get())
            self.current_product_id = product_id
            active_actions, available_actions = self.loop.run_until_complete(self._load_actions(product_id))
            self.update_trees(active_actions, available_actions)
        except ValueError:
            messagebox.showerror("Ошибка", "Введите корректный Product ID")

    async def _load_actions(self, product_id):
        if not self.session:
            await self.init_session()

        if self.cached_actions is None:
            self.cached_actions = await get_actions(self.session)

        all_actions = self.cached_actions
        if not all_actions:
            messagebox.showwarning("Предупреждение", "Не удалось загрузить список акций")
            return [], []

        active_titles = await check_in_actions(self.session, product_id, all_actions)

        available_titles = [title for title in all_actions.keys() if title not in active_titles]

        return active_titles, available_titles

    def update_trees(self, active_actions, available_actions):
        self.active_tree.delete(*self.active_tree.get_children())
        for action in active_actions:
            self.active_tree.insert("", tk.END, values=(action,))

        self.available_tree.delete(*self.available_tree.get_children())
        for action in available_actions:
            self.available_tree.insert("", tk.END, values=(action,))

    def deactivate_selected(self):
        selection = self.active_tree.selection()
        if not selection:
            messagebox.showwarning("Предупреждение", "Выберите акцию для деактивации")
            return

        selected_action = self.active_tree.item(selection[0])['values'][0]
        product_id = self.current_product_id

        if not product_id:
            messagebox.showwarning("Предупреждение", "Сначала загрузите акции для товара")
            return

        result = self.loop.run_until_complete(
            self._deactivate_actions(product_id, [selected_action])
        )

        if result:
            messagebox.showinfo("Успех", f"Акция '{selected_action}' деактивирована")
            self.refresh_actions()
        else:
            messagebox.showerror("Ошибка", "Не удалось деактивировать акцию")

    async def _deactivate_actions(self, product_id, titles):
        return await deactivate_actions(self.session, product_id, self.cached_actions, titles)

    def activate_selected(self):
        selection = self.available_tree.selection()
        if not selection:
            messagebox.showwarning("Предупреждение", "Выберите акцию для активации")
            return

        selected_action = self.available_tree.item(selection[0])['values'][0]
        product_id = self.current_product_id

        if not product_id:
            messagebox.showwarning("Предупреждение", "Сначала загрузите акции для товара")
            return

        result = self.loop.run_until_complete(
            self._activate_actions(product_id, [selected_action], 1000)
        )

        if result:
            messagebox.showinfo("Успех", f"Акция '{selected_action}' активирована")
            self.refresh_actions()
        else:
            messagebox.showerror("Ошибка", "Не удалось активировать акцию")

    async def _activate_actions(self, product_id, titles, price):
        return await activate_actions(self.session, product_id, self.cached_actions, titles, price)

    def refresh_actions(self):
        self.cached_actions = None  # Сброс кэша
        self.load_actions()

def main():
    root = tk.Tk()
    app = ActionManagerApp(root)
    root.mainloop()

if __name__ == "__main__":
    asyncio.run
    (main())