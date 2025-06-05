Це Python-додаток для автоматизованого завантаження, синхронізації та порівняння фінансових даних із різних джерел (DBF, Excel, Finmap API) у базу даних **MS SQL Server**. Має простий графічний інтерфейс (GUI) через Tkinter та підтримує надсилання сповіщень у Telegram.
Можливості
- Обробка `.dbf` файлів та імпорт у SQL.
- Завантаження Excel-файлів з транзакціями.
- Синхронізація між Excel-файлом та SQL-базою.
- Порівняння даних між SQL та Excel.
- Інтеграція з Finmap API та збереження даних у базу.
-  Повідомлення про помилки та успішні операції через Telegram.
Структура
- `watch_folder` — папка з DBF-файлами.
- `excel_folder` — папка з Excel-файлами.
- `excel_path` — файл `datappm.xlsx` для синхронізації.
- `db_config` — параметри підключення до SQL Server.
- `Finmap API` — інтеграція для отримання операцій.
- `Telegram` — налаштування для отримання повідомлень.
 Вимоги
- Python 3.9+
- MS SQL Server
- Пакети Python (можна встановити через `pip install -r requirements.txt`):
```bash
pandas
requests
sqlalchemy
pyodbc
dbfread
openpyxl
xlrd
python-dateutil
tk
