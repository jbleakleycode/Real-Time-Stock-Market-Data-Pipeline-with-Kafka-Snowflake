import json
import time
import requests
import os
from kafka import KafkaProducer
import csv
from datetime import datetime
from io import StringIO
import pytz
from dotenv import load_dotenv
load_dotenv()

# Alpha Vantage API Key (Replace with yours)
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
STOCK_SYMBOL = "IBM"  # Change to any stock symbol
KAFKA_TOPIC = "stock-data"
KAFKA_BROKER = "localhost:9092"

# Set up Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Function to fetch the latest stock data
def fetch_stock_data():
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={STOCK_SYMBOL}&interval=1min&outputsize=compact&apikey={ALPHA_VANTAGE_API_KEY}&datatype=csv"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Error fetching data: {response.status_code}")
        return []

    # Read CSV data from API response
    csv_content = response.text
    csv_reader = csv.reader(StringIO(csv_content))

    # Extract headers and data
    headers = next(csv_reader)  # Read header row
    stock_data = [dict(zip(headers, row)) for row in csv_reader]

    # Add timestamp and stock symbol to each entry
    for row in stock_data:
        row["symbol"] = STOCK_SYMBOL
        row["ingested_at"] = datetime.now(pytz.timezone('America/Toronto')).isoformat()

    return stock_data

# Function to save data to CSV
def save_stock_data(stock_data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"stock_data_{timestamp}.csv"

    with open(csv_filename, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=stock_data[0].keys())
        writer.writeheader()
        writer.writerows(stock_data)

    print(f"CSV file saved as {csv_filename}")

# Fetch and send stock data in a loop
try:
    while True:
        stock_data = fetch_stock_data()
        
        if stock_data:
            # Save data to CSV
            save_stock_data(stock_data)

            # Send each row to Kafka
            for row in stock_data:
                producer.send(KAFKA_TOPIC, row)
                print(f"Sent to Kafka: {row}")

            # Ensure all messages are sent before sleeping
            producer.flush()
        
        time.sleep(900)  # Fetch data every 15 minutes

except KeyboardInterrupt:
    print("\nShutting down producer...")
    producer.close()
    print("Kafka producer closed.")