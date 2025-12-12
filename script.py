import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import re

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
        if len(cols) > 0:
            row_data = [col.get_text(strip=True) for col in cols]
            data_rows.append(row_data)
    
    df = pd.DataFrame(data_rows, columns=headers)
    print(f"Scraped {len(df)} rows of data")
    
    return df

def clean_data(df):    
    df = df.copy()
    
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('/', '_')
    
    revenue_columns = [col for col in df.columns if 'gross' in col or 'revenue' in col]
    for col in revenue_columns:
        if col in df.columns:
            df[col] = df[col].str.replace('$', '').str.replace(',', '')
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    if 'release' in df.columns:
        df['release'] = df['release'].str.strip()
    elif 'title' in df.columns:
        df['title'] = df['title'].str.strip()
    
    critical_columns = [col for col in df.columns if 'rank' in col or 'gross' in col]
    if critical_columns:
        df = df.dropna(subset=[critical_columns[0]])
    
    if 'rank' in df.columns:
        df['rank'] = pd.to_numeric(df['rank'], errors='coerce')
        df['rank'] = df['rank'].astype('Int64')
    
    df['scraped_at'] = pd.Timestamp.now().isoformat()
    
    df = df.dropna(how='all')
    
    print(f"Data cleaned. Final shape: {df.shape}")
    
    return df

def produce_to_kafka(df, topic):
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        
        print(f"Connected to Kafka")
        print(f"Sending messages to topic: {topic}")
        
        success_count = 0
        
        for idx, row in df.iterrows():
            message = row.to_dict()
            
            message = {k: (None if pd.isna(v) else v) for k, v in message.items()}
            
            future = producer.send(topic, value=message)
            
            try:
                record_metadata = future.get(timeout=10)
                success_count += 1
                if (idx + 1) % 10 == 0:
                    print(f"  Sent {idx + 1} messages...")
            except KafkaError as e:
                print(f"Error sending message {idx}: {e}")
        
        producer.flush()
        producer.close()
        
        print(f"Successfully sent {success_count}/{len(df)} messages to Kafka")
        
    except Exception as e:
        print(f"Kafka connection error: {e}")
        raise

def save_data(df, format='csv'):
    if format == 'csv':
        filename = 'cleaned_data.csv'
        df.to_csv(filename, index=False)
    else:
        filename = 'cleaned_data.json'
        df.to_json(filename, orient='records', indent=2)
    
    return filename

def main():
    print("BOX OFFICE MOJO DATA PIPELINE")
    print()
    
    df = scrape_boxoffice_data()
    print()
    
    df_cleaned = clean_data(df)
    print()
    
    print(df_cleaned.head(3))
    print()
    
    try:
        produce_to_kafka(df_cleaned, KAFKA_TOPIC)
        print()
    except Exception as e:
        print(f"Kafka step failed, but continuing to save data")
        print()
    
    save_data(df_cleaned, format='csv')
    save_data(df_cleaned, format='json')
    print()
    
    print("PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"Total rows processed: {len(df_cleaned)}")
    print(f"Files created: cleaned_data.csv, cleaned_data.json")
    print(f"Kafka topic: {KAFKA_TOPIC}")

if __name__ == "__main__":
    main()