import os
import time
import pandas as pd
from faker import Faker
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

INPUT_FOLDER = "./excel_drop"
OUTPUT_FOLDER = "./output_text"

os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

faker = Faker()

def parse_excel(path):
    df = pd.read_excel(path, header=None)
    columns = df.iloc[0].tolist()
    types = df.iloc[1].tolist()
    return columns, types

def generate_fake_row(columns, types, date_format, start_year, end_year, fixed_values={}):
    row = []
    for col, typ in zip(columns, types):
        if col in fixed_values:
            row.append(fixed_values[col])
            continue

        typ_lower = typ.lower()
        if "timestamp" in typ_lower:
            start_date = datetime(start_year, 1, 1)
            end_date = datetime(end_year, 12, 31)
            dt = faker.date_time_between(start_date=start_date, end_date=end_date)
            row.append(dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        elif "date" in typ_lower:
            start_date = datetime(start_year, 1, 1)
            end_date = datetime(end_year, 12, 31)
            value = faker.date_between(start_date=start_date, end_date=end_date)
            row.append(value.strftime(date_format))
        elif "int" in typ_lower or "smallint" in typ_lower or "bigint" in typ_lower:
            row.append(faker.random_int(min=0, max=99999))
        elif "float" in typ_lower or "decimal" in typ_lower:
            row.append(round(faker.pyfloat(left_digits=4, right_digits=2, positive=True), 2))
        elif "email" in col.lower():
            row.append(faker.email())
        elif "char" in typ_lower or "varchar" in typ_lower or "string" in typ_lower:
            row.append(faker.word())
        else:
            row.append(faker.word())
    return row

def generate_unique_data(columns, types, count, date_format, start_year, end_year, pk_indices, fixed_values={}):
    data = []
    seen_keys = set()
    attempts = 0
    max_attempts = count * 10

    while len(data) < count and attempts < max_attempts:
        row = generate_fake_row(columns, types, date_format, start_year, end_year, fixed_values)
        key = tuple(row[i] for i in pk_indices)
        if key not in seen_keys:
            seen_keys.add(key)
            data.append(row)
        attempts += 1

    if len(data) < count:
        print(f"⚠️ Warning: Only generated {len(data)} unique rows out of requested {count}.")
    return data

class ExcelHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".xlsx"):
            file_path = event.src_path
            print(f"\n📥 New Excel file detected: {file_path}")
            try:
                columns, types = parse_excel(file_path)
                print(f"🧾 Columns: {columns}")
                print(f"🔢 Types: {types}")

                num_rows = int(input("🔢 How many rows of data should be generated? "))
                date_format = input("📆 What date format should be used? (e.g. %Y-%m-%d, %d/%m/%Y): ")
                delimiter = input("🔣 What character should be used as a delimiter between rows? (e.g. ',' or ';' or '|'): ")
                start_year = int(input("📅 Start year for date fields? "))
                end_year = int(input("📅 End year for date fields? "))

                print("🗝️ Columns:")
                for idx, name in enumerate(columns):
                    print(f"{idx}: {name}")
                pk_input = input("🔑 Enter comma-separated column numbers that form the Primary Key (e.g., 0,2): ")
                pk_indices = [int(i.strip()) for i in pk_input.split(",") if i.strip().isdigit()]

                fixed_values = {}
                if "ENTITY_PRC" in columns:
                    val = input("🏢 What fixed value should be used for 'ENTITY_PRC'? ")
                    fixed_values["ENTITY_PRC"] = val

                print("🤖 Generating data...")

                rows = generate_unique_data(columns, types, num_rows, date_format, start_year, end_year, pk_indices, fixed_values)

                if not rows:
                    print("❌ No data generated. Skipping file.")
                    return

                df = pd.DataFrame(rows, columns=columns)
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
