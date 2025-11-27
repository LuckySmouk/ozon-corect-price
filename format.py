import pandas as pd
import os


class ProductFinder:
    """
    Класс для поиска строк в Excel-файле по списку идентификаторов.
    Поддерживает поиск по полям: 'Ozon Product ID', 'SKU', 'Артикул'.
    """

    def __init__(self, input_file_path, id_list_path, output_file_path):
        """
        :param input_file_path: Путь к исходному Excel-файлу
        :param id_list_path: Путь к файлу со списком идентификаторов
        :param output_file_path: Путь для сохранения результата
        """
        self.input_file_path = input_file_path
        self.id_list_path = id_list_path
        self.output_file_path = output_file_path
        self.product_ids = set()  # Для хранения уникальных идентификаторов
        self.result_df = None     # Для хранения результата поиска

    def load_id_list(self):
        """Загружает список идентификаторов из текстового файла."""
        try:
            with open(self.id_list_path, 'r', encoding='utf-8') as file:
                for line in file:
                    stripped_line = line.strip()
                    if stripped_line:
                        self.product_ids.add(stripped_line)
            print(f"[INFO] Загружено {len(self.product_ids)} идентификаторов.")
        except Exception as e:
            print(f"[ERROR] Ошибка при чтении файла {self.id_list_path}: {e}")
            raise

    def find_matching_rows(self):
        """Читает Excel-файл и находит строки, где значения совпадают с идентификаторами."""
        try:
            df = pd.read_excel(self.input_file_path)

            # Проверяем наличие нужных столбцов
            required_columns = ['Ozon Product ID', 'SKU', 'Артикул']
            available_columns = [col for col in required_columns if col in df.columns]
            if not available_columns:
                raise KeyError("Нет ни одного из ожидаемых столбцов: Ozon Product ID, SKU, Артикул")

            # Создаем маску для фильтрации строк
            mask = df[available_columns].apply(lambda row: row.astype(str).str.contains('|'.join(self.product_ids), case=False).any(), axis=1)
            self.result_df = df[mask]

            print(f"[INFO] Найдено {len(self.result_df)} совпадений.")
        except Exception as e:
            print(f"[ERROR] Ошибка при обработке данных из файла {self.input_file_path}: {e}")
            raise

    def save_result(self):
        """Сохраняет результат поиска в новый Excel-файл, если данные найдены."""
        if self.result_df is None:
            print("[ERROR] Нет данных для сохранения. Возможно, поиск не был выполнен или совпадений не найдено.")
            return

        try:
            # Создаем директорию, если её нет
            os.makedirs(os.path.dirname(self.output_file_path), exist_ok=True)

            # Сохраняем результат
            self.result_df.to_excel(self.output_file_path, index=False)
            print(f"[INFO] Результат сохранён в файл: {self.output_file_path}")
        except Exception as e:
            print(f"[ERROR] Ошибка при сохранении файла: {e}")
            raise

    def run(self):
        """Запуск процесса обработки."""
        self.load_id_list()
        self.find_matching_rows()
        self.save_result()


# Точка входа
if __name__ == "__main__":
    finder = ProductFinder(
        input_file_path='in/products_update_full.xlsx',
        id_list_path='get/get_new.txt',
        output_file_path='in/1_1_product.xlsx'
    )
    finder.run()