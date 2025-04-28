import os
import time
import json
import pandas as pd
from openai import OpenAI
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key="")

INPUT_FOLDER = "./excel_in"
OUTPUT_FOLDER = "./output_text"

os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def clean_json(text):
    try:
        return json.loads(text)
    except:
        try:
            start = text.index("[")
            end = text.rindex("]") + 1
            return json.loads(text[start:end])
        except:
            return None

def generate_rows(columns, types, total_count, start_year, end_year, date_format, primary_key_indexes, retries=3, chunk_size=50):
    all_rows = []
    unique_keys = set()

    while len(all_rows) < total_count:
        remaining = total_count - len(all_rows)
        batch = min(remaining, chunk_size)
        print(f"🔄 Generating batch of {batch} rows (done: {len(all_rows)}/{total_count})")
        partial = _generate_single_batch(columns, types, batch, start_year, end_year, date_format, retries)
        
        if not partial:
            print("❌ Failed to generate a batch. Stopping.")
            break

        for row in partial:
            key = tuple(str(row.get(columns[idx], "")) for idx in primary_key_indexes)
            if key not in unique_keys:
                unique_keys.add(key)
                all_rows.append(row)

        if batch == 0:
            break

    return all_rows[:total_count]

def _generate_single_batch(columns, types, count, start_year, end_year, date_format, retries=3):
    schema = "\n".join([f"- {col} ({typ})" for col, typ in zip(columns, types)])
    system = {"role": "system", "content": "You are a data generator that produces realistic tabular data in JSON format. Do NOT include code blocks or explanation."}
    user = {
        "role": "user",
        "content": (
            f"Generate exactly {count} rows of fake data based on this schema:\n"
            f"{schema}\n"
            f"For any DATE type, generate dates between {start_year} and {end_year}.\n"
            f"Use the date format: {date_format}.\n"
            f"Respond with only a JSON array of objects. No code block markers."
        )
    }

    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[system, user],
                temperature=0.6
            )
            raw = response.choices[0].message.content
            rows = clean_json(raw)
            if rows and isinstance(rows, list):
                return rows
        except Exception as e:
            print(f"⚠️ Error during batch generation: {e}")
    return []   

def parse_excel(path):
    df = pd.read_excel(path, header=None)
    columns = df.iloc[0].tolist()
    types = df.iloc[1].tolist()
    return columns, types

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

                success = False
                while not success:
                    print("🤖 Generating data...")
                    data = generate_rows(columns, types, num_rows, start_year, end_year, date_format, primary_key_indexes)

                    if not data:
                        print("❌ No valid data generated. Skipping file...")
                        return

                    if len(data) < num_rows:
                        retry = input(f"⚠️ Only {len(data)} unique rows generated instead of {num_rows}. Retry with different PK? (y/n): ").lower()
                        if retry == "y":
                            primary_key_input = input("\n🔑 Enter the NEW column numbers for Primary Key, separated by commas: ")
                            primary_key_indexes = [int(x.strip()) for x in primary_key_input.split(",")]
                        else:
                            print("✅ Proceeding with available data.")
                            success = True
                    else:
                        success = True

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
