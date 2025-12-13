import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

URL = "https://www.boxofficemojo.com/year/2024/"
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'bonus_22B030358'

def scrape_boxoffice_data():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    response = requests.get(URL, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find('table')
    if not table:
        raise Exception("Table not found on the page")

    headers_row = table.find('tr')
    headers = [th.get_text(strip=True) for th in headers_row.find_all('th')]

    data_rows = []
    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        if cols:
            data_rows.append([col.get_text(strip=True) for col in cols])

    df = pd.DataFrame(data_rows, columns=headers)
    print(f"Scraped {len(df)} rows")

    return df

def clean_data(df):
    df = df.copy()

    df.columns = (
        df.columns
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('/', '_')
    )

    revenue_columns = [c for c in df.columns if 'gross' in c]
    for col in revenue_columns:
        df[col] = (
            df[col]
            .str.replace('$', '', regex=False)
            .str.replace(',', '', regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'rank' in df.columns:
        df['rank'] = pd.to_numeric(df['rank'], errors='coerce')

    if 'release' in df.columns:
        df['release'] = df['release'].str.strip()

    df['scraped_at'] = pd.Timestamp.now().isoformat()


    df = df.dropna(axis=1, how='all')

    df = df.loc[:, ~(df == '-').all()]

    print(f"Data cleaned. Final shape: {df.shape}")

    return df

def produce_to_kafka(df, topic):
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )

    success = 0

    for _, row in df.iterrows():
        message = {
            k: (None if pd.isna(v) else v)
            for k, v in row.to_dict().items()
        }

        try:
            producer.send(topic, value=message).get(timeout=10)
            success += 1
        except KafkaError as e:
            print(f"Kafka error: {e}")

    producer.flush()
    producer.close()

    print(f"Sent {success}/{len(df)} messages to Kafka")

def save_data(df):
    df.to_csv('cleaned_data.csv', index=False)
    df.to_json('cleaned_data.json', orient='records', indent=2)
    print("Files saved: cleaned_data.csv, cleaned_data.json")

def main():
    print("BOX OFFICE MOJO DATA PIPELINE\n")

    df_raw = scrape_boxoffice_data()
    print()

    df_cleaned = clean_data(df_raw)
    print(df_cleaned.head(3))
    print()

    try:
        produce_to_kafka(df_cleaned, KAFKA_TOPIC)
    except Exception as e:
        print("Kafka step failed:", e)

    save_data(df_cleaned)

    print("\nPIPELINE COMPLETED SUCCESSFULLY")
    print(f"Total rows processed: {len(df_cleaned)}")
    print(f"Kafka topic: {KAFKA_TOPIC}")


if __name__ == "__main__":
    main()
