import json
import os
import tempfile
from datetime import datetime
import time
from kafka import KafkaConsumer
from dotenv import load_dotenv
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytz
import uuid

load_dotenv()

# AWS S3 Config (Using environment variables for security)
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = "stock-info-s3-555"

# Kafka Config
KAFKA_TOPIC = "stock-data"
KAFKA_BROKER = "localhost:9092"

# Initializes S3 Client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# Checks if the stock_data message contains required fields
def is_valid_stock_data(stock_data):
    """Check if the stock_data message contains required fields."""
    return "symbol" in stock_data and "ingested_at" in stock_data

# Processes a Kafka message by validating and returning the stock data
def process_message(message):
    """
    Process a Kafka message by validating and returning the stock data.
    Returns None if the message is malformed.
    """
    stock_data = message.value
    if not is_valid_stock_data(stock_data):
        print("Skipping malformed message:", stock_data)
        return None
    return stock_data

# Converts a list of stock data dictionaries into a Parquet file
def convert_to_parquet(stock_records, output_path):
    """
    Convert a list of stock data dictionaries into a Parquet file.
    
    Args:
        stock_records (list): List of dictionaries with stock data.
        output_path (str): File path to save the Parquet file.
    
    Returns:
        str: The path of the generated Parquet file.
    """
    df = pd.DataFrame(stock_records)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path)
    return output_path

def upload_file_to_s3(file_path, s3_key):
    """
    Upload a local file to an S3 bucket.
    
    Args:
        file_path (str): Local path of the file.
        s3_key (str): S3 key (path) to store the file.
    """
    with open(file_path, 'rb') as f:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=f,
            ContentType="application/octet-stream"
        )
    print(f"✅ Uploaded {s3_key} to S3")

def generate_s3_key(symbol, batch_time, consumer_id):
    """
    Generate a safe S3 key for the Parquet file.
    
    Args:
        symbol (str): Stock symbol (assumes all records in the batch share the same symbol or use a generic label).
        batch_time (datetime): Timestamp to incorporate into the file name.
    
    Returns:
        str: Generated S3 key.
    """
    safe_timestamp = batch_time.strftime("%Y-%m-%dT%H_%M_%SZ")
    return f"stock_data/{symbol}_{safe_timestamp}_{consumer_id}.parquet"

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="stocks_consumer_group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest"
    )

    # Generate a unique ID for this consumer instance.
    consumer_id = str(uuid.uuid4())[:8]
    print(f"Consumer instance ID: {consumer_id}")
    
    batch_size = 10  # Adjust batch size as needed
    records_batch = []
    batch_start_time = datetime.now(pytz.timezone('America/Toronto'))
    
    print("Kafka consumer started. Waiting for messages...")
    try:
        while True:
            # Poll for new messages every 1 second
            records = consumer.poll(timeout_ms=1000)
            if records:
                for tp, messages in records.items():
                    for message in messages:
                        stock_data = process_message(message)
                        if stock_data:
                            records_batch.append(stock_data)
                            print(f"Received valid message. Batch size: {(len(records_batch) % 10)}/{batch_size}")
                            
                # Once batch_size is reached, process the batch
                if len(records_batch) >= batch_size:
                    symbol = records_batch[0].get("symbol", "generic")
                    
                    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
                        parquet_path = tmp_file.name
                    
                    convert_to_parquet(records_batch, parquet_path)
                    s3_key = generate_s3_key(symbol, batch_start_time, consumer_id)
                    upload_file_to_s3(parquet_path, s3_key)
                    
                    records_batch = []
                    batch_start_time = datetime.now(pytz.timezone('America/Toronto'))
                    print("Batch processed and uploaded. Waiting for next batch...")
            else:
                time.sleep(1)
                
    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    finally:
        consumer.close()
        print("Kafka consumer closed.")

if __name__ == "__main__":
    main()


# import json
# import boto3
# import os
# from kafka import KafkaConsumer
# from dotenv import load_dotenv
# load_dotenv()

# # AWS S3 Config (Use environment variables for security)
# AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
# AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# S3_BUCKET_NAME = "stock-info-s3-555"

# # Kafka Config
# KAFKA_TOPIC = "stock-data"
# KAFKA_BROKER = "localhost:9092"

# # Initialize Kafka Consumer
# consumer = KafkaConsumer(
#     KAFKA_TOPIC,
#     bootstrap_servers=KAFKA_BROKER,
#     value_deserializer=lambda v: json.loads(v.decode("utf-8"))
# )

# # Initialize S3 Client
# s3_client = boto3.client(
#     "s3",
#     aws_access_key_id=AWS_ACCESS_KEY,
#     aws_secret_access_key=AWS_SECRET_KEY
# )

# # Process messages from Kafka
# try:
#     for message in consumer:
#         try:
#             stock_data = message.value

#             # Ensure the expected data structure exists
#             if "symbol" not in stock_data or "ingested_at" not in stock_data:
#                 print("Skipping malformed message:", stock_data)
#                 continue

#             # Sanitize timestamp for S3-friendly filenames
#             safe_timestamp = stock_data["ingested_at"].replace(":", "_")

#             # Define S3 file path
#             file_name = f"stock_data/{stock_data['symbol']}_{safe_timestamp}.json"

#             # Upload to S3
#             s3_client.put_object(
#                 Bucket=S3_BUCKET_NAME,
#                 Key=file_name,
#                 Body=json.dumps(stock_data),
#                 ContentType="application/json"
#             )

#             print(f"✅ Uploaded {file_name} to S3")

#         except Exception as e:
#             print(f"❌ Error processing message: {e}")

# except KeyboardInterrupt:
#     print("\nShutting down consumer...")
#     consumer.close()
#     print("Kafka consumer closed.")