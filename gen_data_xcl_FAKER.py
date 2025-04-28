import os
import time
import json
import pandas as pd
from faker import Faker
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv
import random
from datetime import datetime

load_dotenv()

INPUT_FOLDER = "./excel_in"
OUTPUT_FOLDER = "./output_text"

fake = Faker()
os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def parse_excel(path):
    df = pd.read_excel(path, header=None)
    columns = df.iloc[0].tolist()
    types = df.iloc[1].tolist()
    return columns, types

def generate_fake_value(col_type, start_year, end_year, date_format):
    if "SMALLINT" in col_type.upper():
        return random.randint(-32768, 32767)
    elif "INTEGER" in col_type.upper():
        return random.randint(-2147483648, 2147483647)
    elif "BIGINT" in col_type.upper():
        return random.randint(-9223372036854775808, 9223372036854775807)
    elif "FLOAT" in col_type.upper():
        precision = int(col_type[col_type.find("(")+1:col_type.find(")")]) if "(" in col_type else 2
        return round(random.uniform(-1e5, 1e5), precision)
    elif "DECIMAL" in col_type.upper():
        return round(random.uniform(-1e5, 1e5), 2)
    elif "DATE" in col_type.upper():
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)
        return fake.date_between(start_date=start_date, end_date=end_date).strftime(date_format)
    elif "STRING" in col_type.upper():
        length = int(col_type[col_type.find("(")+1:col_type.find(")")]) if "(" in col_type else 20
        return fake.text(max_nb_chars=length).strip().replace("\n", " ")
    else:
        return fake.word()

def generate_rows(columns, types, total_count, start_year, end_year, date_format, primary_key_indexes):
    all_rows = []
    unique_keys = set()

    attempts = 0
    while len(all_rows) < total_count and attempts < total_count * 5:
        row = {col: generate_fake_value(typ, start_year, end_year, date_format) for col, typ in zip(columns, types)}
        key = tuple(str(row.get(columns[idx], "")) for idx in primary_key_indexes)
        if key not in unique_keys:
            unique_keys.add(key)
            all_rows.append(row)
        attempts += 1

    return all_rows

class ExcelHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".xlsx"):
            file_path = event.src_path
            print(f"\n📥 New Excel file detected: {file_path}")
            try:
                columns, types = parse_excel(file_path)
                print(f"Columns: {columns}")
                print(f"Types: {types}")

                num_rows = int(input("🔢 How many rows of data should be generated? "))
                start_year = int(input("📅 Start year for date fields? "))
                end_year = int(input("📅 End year for date fields? "))
                date_format = input("📆 What date format should be used? (e.g. %Y-%m-%d, %d/%m/%Y): ")
                delimiter = input("🔣 What character should be used as a delimiter between rows? (e.g. ',' or ';' or '|'): ")

                print("\n🛡️ Column indexes:")
                for i, col in enumerate(columns):
                    print(f"  {i}: {col}")
                primary_key_input = input("\n🔑 Enter the column numbers that form the Primary Key, separated by commas (e.g., 0,2): ")
                primary_key_indexes = [int(x.strip()) for x in primary_key_input.split(",")]

                print("🤖 Generating fake data...")

                data = generate_rows(columns, types, num_rows, start_year, end_year, date_format, primary_key_indexes)

                if len(data) < num_rows:
                    print(f"⚠️ Only {len(data)} unique rows generated out of {num_rows} requested due to PK constraints.")

                df = pd.DataFrame(data)
                filename = os.path.basename(file_path).replace(".xlsx", ".txt")
                output_path = os.path.join(OUTPUT_FOLDER, filename)

                df.to_csv(output_path, index=False, sep=delimiter, lineterminator="\n")
                print(f"✅ Data saved to {output_path}")
            except Exception as e:
                print(f"❌ Error processing file: {e}")

def main():
    print(f"📂 Watching folder: {INPUT_FOLDER}")
    observer = Observer()
    observer.schedule(ExcelHandler(), INPUT_FOLDER, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
