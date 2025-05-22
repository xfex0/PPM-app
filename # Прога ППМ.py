# Прога ППМ
import os
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dbfread import DBF
import tkinter as tk
from tkinter import messagebox
from datetime import datetime
from dateutil import parser

# === Налаштування ===
watch_folder = r"C:\\Users\\Тарас\\source\\Виписки"
excel_folder = r"C:\\Users\\Тарас\\source\\Excel"
excel_path = r"C:\\Users\\Тарас\\Desktop\\datappm.xlsx"
server = 'TARIK-LAPTOP'
database = 'PPM_Group'
username = 'NewLogin'
password = '123'
table_name_dbf = 'Table_1'
table_name_excel = 'ExcelTransactions_New'
table_name_datappm = 'datappm'
table_name_finmap = 'FinmapTransactions'
TOKEN = "6993434961:AAEGyvYPwr4ouic_jHA5L5yErgu0RAKAljs"
CHAT_ID = "-1002545826247"

# === Finmap API ===
finmap_url = "https://api.finmap.online/v2.2/operations/list"
finmap_headers = {
    "apiKey": "0b6938ce-d081-4a32-acf2-84127d976142152352516fe05dbadcc23493187424afc24d4f1c"
}

# === Підключення через SQLAlchemy ===
connection_string = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
)
engine = create_engine(connection_string, fast_executemany=True)

# === Telegram ===
def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": message})
    except Exception as e:
        print(f"❌ Telegram error: {e}")

# === DBF ===
def read_dbf_to_df(file_path):
    try:
        table = DBF(file_path, encoding='cp1251')
        df = pd.DataFrame(iter(table))
        return df
    except Exception as e:
        print(f"❌ DBF read error: {e}")
        return None

def get_all_new_dbf_files(folder_path):
    return sorted([
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.lower().endswith('.dbf')
    ], key=os.path.getmtime)

def convert_date_column(df, column_name="DATE"):
    try:
        if column_name in df.columns:
            df[column_name] = pd.to_datetime(df[column_name], errors='coerce', dayfirst=True)
    except Exception as e:
        print(f"❗ Помилка при конвертації дати: {e}")
    return df

def upload_dbf_to_sql(file_path):
    df = read_dbf_to_df(file_path)
    if df is None or df.empty:
        print(f"⚠️ Порожній або помилковий DBF: {file_path}")
        return

    df = convert_date_column(df, "DATE")
    df["source_file"] = os.path.basename(file_path)

    try:
        with engine.begin() as conn:
            for _, row in df.iterrows():
                placeholders = ', '.join(f":{col}" for col in df.columns)
                columns = ', '.join(df.columns)
                query = text(f"""
                    INSERT INTO {table_name_dbf} ({columns})
                    VALUES ({placeholders})
                """)
                params = {col: row[col] for col in df.columns}
                conn.execute(query, params)
        print(f"✅ DBF успішно завантажено: {file_path}")
    except SQLAlchemyError as e:
        print(f"❌ DBF SQL insert error: {e}")

def process_all_dbf_files():
    files = get_all_new_dbf_files(watch_folder)
    for file_path in files:
        upload_dbf_to_sql(file_path)
        
# === Excel у папці ===
def read_excel_and_upload(folder_path):
    if not os.path.exists(folder_path):
        print(f"❌ Папка не знайдена: {folder_path}")
        send_telegram_message(f"❌ Excel-папку не знайдено: {folder_path}")
        return

    for file in os.listdir(folder_path):
        if not file.endswith(('.xls', '.xlsx')):
            continue
        file_path = os.path.join(folder_path, file)

        with engine.connect() as conn:
            res = conn.execute(
                text(f"SELECT COUNT(*) FROM {table_name_excel} WHERE source_file = :source_file"),
                {"source_file": file}
            )
            count = res.scalar()
            if count > 0:
                print(f"⏭️ Excel вже оброблений: {file}")
                send_telegram_message(f"✅ Excel завантажено: {file}")
                continue

        try:
            df = pd.read_excel(file_path, header=None, engine='xlrd')
            data = []
            for i in range(1, len(df)):
                # === Перетворення дати ===
                date = pd.to_datetime(df.iloc[i, 1], dayfirst=True, errors='coerce')
                if pd.notna(date):
                    date = date.date()  # ← перетворення до формату YYYY-MM-DD без часу

                amount = pd.to_numeric(df.iloc[i, 3], errors='coerce')
                amount = round(amount, 6) if pd.notna(amount) else None
                currency = str(df.iloc[i, 4])
                purpose = str(df.iloc[i, 5])
                counterparty = str(df.iloc[i, 7])
                type_ = "Надходження" if amount and amount > 0 else "Переказ"

                if pd.isna(date) or pd.isna(amount):
                    continue

                data.append({
                    "date": date,
                    "amount": amount,
                    "currency": currency,
                    "purpose": purpose,
                    "counterparty": counterparty,
                    "type": type_,
                    "source_file": file
                })

            with engine.begin() as conn:
                for row in data:
                    conn.execute(
                        text(f"""
                            INSERT INTO {table_name_excel} 
                            (date, amount, currency, purpose, counterparty, type, source_file)
                            VALUES (:date, :amount, :currency, :purpose, :counterparty, :type, :source_file)
                        """),
                        row
                    )
            print(f"✅ Excel завантажено: {file}")
        except Exception as e:
            print(f"❌ Помилка при обробці Excel: {file}, {e}")
            send_telegram_message(f"❌ Помилка при обробці Excel: {file}, {e}")


# === Синхронізація datappm Excel → SQL ===
def sync_excel_to_sql():
    if not os.path.exists(excel_path):
        print(f"❌ Excel файл не знайдено: {excel_path}")
        send_telegram_message(f"❌ Excel файл не знайдено: {excel_path}")
        return

    df = pd.read_excel(excel_path)
    df.columns = [col.strip().replace(" ", "_") for col in df.columns]
    sql_columns = df.columns.tolist()

    for col in sql_columns:
        if 'amount' in col.lower() or 'sum' in col.lower():
            df[col] = pd.to_numeric(df[col], errors='coerce').round(6)

    columns_str = ', '.join([f"[{col}]" for col in sql_columns])
    placeholders = ', '.join([f":{col}" for col in sql_columns])

    inserted_count = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            project_val = row.get("project", None)
            if not project_val:
                continue

            res = conn.execute(
                text(f"SELECT COUNT(*) FROM {table_name_datappm} WHERE project = :project"),
                {"project": project_val}
            )
            if res.scalar() > 0:
                continue

            params = {col: row[col] for col in sql_columns}
            try:
                conn.execute(
                    text(f"""
                        INSERT INTO {table_name_datappm} ({columns_str})
                        VALUES ({placeholders})
                    """),
                    params
                )
                inserted_count += 1
            except SQLAlchemyError as e:
                print(f"⚠️ Пропущено рядок: {e}")

    send_telegram_message(f"✅ Додано до SQL: {inserted_count} рядків")

# === Синхронізація SQL → datappm Excel ===
def sync_sql_to_excel():
    df_sql = pd.read_sql(f"SELECT * FROM {table_name_datappm}", engine)
    df_sql.columns = [col.strip().replace(" ", "_") for col in df_sql.columns]

    if os.path.exists(excel_path):
        df_excel = pd.read_excel(excel_path)
        df_excel.columns = [col.strip().replace(" ", "_") for col in df_excel.columns]

        if "project" not in df_excel.columns:
            print("❌ Excel не має колонки 'project'")
            send_telegram_message("❌ Excel не має колонки 'project'")
            return

        new_rows = df_sql[~df_sql["project"].isin(df_excel["project"])]
        if not new_rows.empty:
            df_combined = pd.concat([df_excel, new_rows], ignore_index=True)
            df_combined.to_excel(excel_path, index=False)
            send_telegram_message(f"🔄 Excel оновлено: додано {len(new_rows)} нових рядків")
        else:
            send_telegram_message("ℹ️ Дані з SQL вже є в Excel. Нових рядків не знайдено.")
    else:
        df_sql.to_excel(excel_path, index=False)
        send_telegram_message(f"📄 Excel створено з SQL ({len(df_sql)} рядків)")

# === Порівняння Excel і SQL ===
def compare_excel_sql():
    if not os.path.exists(excel_path):
        send_telegram_message("❌ Excel файл не знайдено.")
        return

    df_excel = pd.read_excel(excel_path)
    df_excel.columns = [col.strip().replace(" ", "_") for col in df_excel.columns]

    df_sql = pd.read_sql(f"SELECT * FROM {table_name_datappm}", engine)
    df_sql.columns = [col.strip().replace(" ", "_") for col in df_sql.columns]

    excel_cols = set(df_excel.columns)
    sql_cols = set(df_sql.columns)
    only_in_excel = excel_cols - sql_cols
    only_in_sql = sql_cols - excel_cols

    new_in_excel = df_excel[~df_excel["project"].isin(df_sql["project"])]
    new_in_sql = df_sql[~df_sql["project"].isin(df_excel["project"])]

    message = "📊 *Порівняння Excel і SQL:*\n"
    if only_in_excel:
        message += f"➕ *У Excel, але не в SQL:* {', '.join(only_in_excel)}\n"
    if only_in_sql:
        message += f"➕ *У SQL, але не в Excel:* {', '.join(only_in_sql)}\n"
    message += f"📈 Нові рядки у Excel (відсутні в SQL): {len(new_in_excel)}\n"
    message += f"📉 Нові рядки в SQL (відсутні в Excel): {len(new_in_sql)}"

    send_telegram_message(message)

# === Завантаження Finmap даних у SQL ===
def load_finmap_to_sql():
    try:
        payload = {"filters": {}, "page": 1, "limit": 100}
        response = requests.post(finmap_url, headers=finmap_headers, json=payload)
        response.raise_for_status()
        data = response.json()
        operations = data.get('list', [])

        if not operations:
            send_telegram_message("ℹ️ Finmap: Дані операцій відсутні.")
            return

        with engine.begin() as conn:
            inserted_count = 0
            for op in operations:
                # === Обробка дати ===
                raw_date = op.get('date')
                date = None
                try:
                    if isinstance(raw_date, (int, float)):
                        date = datetime.fromtimestamp(raw_date / 1000)
                    elif isinstance(raw_date, str):
                        date = parser.parse(raw_date)
                except Exception as e:
                    print(f"❗ Неможливо розпарсити дату: {raw_date} — {e}")
                    continue

                # === Інші поля ===
                amount = op.get('sum', 0)

                project = op.get('project') or ''
                account_info = op.get('account', {})
                account = account_info.get('title', '') or ''
                balance = account_info.get('balance')
                if isinstance(balance, str):
                    try:
                        balance = float(balance.replace(' ', '').replace(',', '.'))
                    except:
                        balance = None

                counterparty = op.get('counterparty', {}).get('title', '') or ''
                category = op.get('category', {}).get('title', '') or ''
                description = op.get('comment') or ''
                currency = op.get('currency') or ''

                # === Унікальність — з урахуванням balance ===
                exists_query = text(f"""
                    SELECT COUNT(*) FROM {table_name_finmap}
                    WHERE 
                        date = :date AND 
                        amount = :amount AND 
                        project = :project AND 
                        account = :account AND
                        counterparty = :counterparty AND
                        category = :category AND 
                        description = :description AND 
                        currency = :currency AND
                        (balance = :balance OR (balance IS NULL AND :balance IS NULL))
                """)
                res = conn.execute(exists_query, {
                    "date": date,
                    "amount": amount,
                    "project": project,
                    "account": account,
                    "counterparty": counterparty,
                    "category": category,
                    "description": description,
                    "currency": currency,
                    "balance": balance
                })

                if res.scalar() > 0:
                    continue

                # === Вставка ===
                insert_query = text(f"""
                    INSERT INTO {table_name_finmap} 
                        (date, amount, project, account, counterparty, category, description, currency, balance)
                    VALUES 
                        (:date, :amount, :project, :account, :counterparty, :category, :description, :currency, :balance)
                """)
                conn.execute(insert_query, {
                    "date": date,
                    "amount": amount,
                    "project": project,
                    "account": account,
                    "counterparty": counterparty,
                    "category": category,
                    "description": description,
                    "currency": currency,
                    "balance": balance
                })
                inserted_count += 1

        send_telegram_message(f"✅ Finmap: Завантажено {inserted_count} нових операцій")

    except Exception as e:
        send_telegram_message(f"❌ Finmap API error: {e}")
# === Tkinter GUI ===
def run_all_tasks():
    try:
        # DBF
        dbf_files = get_all_new_dbf_files(watch_folder)
        if dbf_files:
            for file in dbf_files:
                upload_dbf_to_sql(file)
        else:
            print("ℹ️ DBF файлів для обробки немає.")

        # Excel з папки
        read_excel_and_upload(excel_folder)

        # Синхронізація Excel → SQL
        sync_excel_to_sql()

        # Синхронізація SQL → Excel
        sync_sql_to_excel()

        # Порівняння Excel і SQL
        compare_excel_sql()

        # Finmap
        load_finmap_to_sql()

        messagebox.showinfo("Успіх", "Усі завдання виконані!")
    except Exception as e:
        messagebox.showerror("Помилка", str(e))

def create_gui():
    root = tk.Tk()
    root.title("Завантаження даних у SQL")
    root.geometry("400x200")

    label = tk.Label(root, text="Натисніть кнопку для запуску всіх процесів", font=("Arial", 12))
    label.pack(pady=20)

    btn = tk.Button(root, text="Запустити", command=run_all_tasks, font=("Arial", 14), bg="green", fg="white")
    btn.pack(pady=10)

    root.mainloop()

if __name__ == "__main__":
    create_gui()
