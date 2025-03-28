﻿# Real-Time-Stock-Market-Data-Pipeline-with-Kafka-Snowflake

The purpose of this project is to learn how to upload real-time data to S3 using Kafka (in a docker container), using local producers and consumers in Python.

To set up this project:
 - Docker must be installed
 - Python must be installed on chosed IDE (VScode recommended)
 - An AlphaVantage API key must be acquired and added to the project:
    1. Enter details to acquire API key from "https://www.alphavantage.co/support/#api-key" (this is free)
    2. Create ".env" file in the project root folder
    3. Add "ALPHA_VANTAGE_API_KEY = <api key>" to the .env file
 - Create a dedicated S3 bucket and IAM user:
    1. Create an S3 bucket
    2. Create an IAM user with an appropriate name, e.g. "kafka-s3-uploader"
    3. Add permission for the user:
      - Go to user page
      - Go to "Add permissions"
      - Choose the "Create inline policy" option
      - Change the policy editor view to "JSON"
      - Copy the contents from "amazon_s3_bucker_user_policy.json" and paste it into the inline policy editor
      - Edit the bucket name where it says "your bucket name"
      - Hit "Next" and "Create policy" to finalise the new permissions for the user
    4. Acquire and add the user access key & secret to the .env file:
      - Go to the user page
      - Hit "Create access key" in the Summary section
      - Choose "Local Code"
      - Give the key an appropriate description and hit "Create access key"
      - Download the CSV file to store the access key and secret
      - Add "<your user> access key: <your access key>" & "<your user> secret: <your secret>" to the .env file in the project root folder
    5. Create a virtual environment to run the python scripts: "python -m venv venv" or "python3 -m venv venv" (macOS)
      - Activate the virtual environment: "venv\scripts\activate" or "source venv/bin/activate" (macOS)
      - Install required dependencies: "pip install -r requirements.txt"

To run this project:
 - run "docker-compose up -d" to start Kafka in docker
 - Activate the virtual environment: "venv\scripts\activate" or "source venv/bin/activate" (macOS)
 - Run the kafka producer: "python kafka-producer.py"
 - Run the consumer: "python kafka-consumer.py"

# Real-Time Stock Market Data Pipeline with Kafka & Snowflake

This project demonstrates how to upload real-time data to S3 using Kafka (running in Docker) and Python-based producers and consumers.

---

## How to Run

To run this project:
 - run "docker-compose up -d" to start Kafka in docker
 - Activate the virtual environment: "venv\scripts\activate" or "source venv/bin/activate" (macOS)
 - Run the kafka producer: "python kafka-producer.py"
 - Run the consumer: "python kafka-consumer.py"

## Prerequisites

Before setting up the project, ensure you have:

- **Docker** installed.
- **Python** installed (using an IDE like VSCode is recommended).
- An **AlphaVantage API Key**.
- An **AWS S3 Bucket** and an **IAM User** with proper permissions.

---

## Setup Instructions

### 1. AlphaVantage API Key

1. **Obtain an API Key:**
   - Visit [AlphaVantage API Key Request](https://www.alphavantage.co/support/#api-key) to get a free API key.
   
2. **Configure the API Key:**
   - Create a `.env` file in the project's root directory.
   - Add the following line, replacing `<api key>` with your actual key:
     ```env
     ALPHA_VANTAGE_API_KEY=<api key>
     ```

---

### 2. AWS S3 Bucket and IAM User

#### Create an S3 Bucket
- Set up a dedicated S3 bucket for the project.

#### Create an IAM User (e.g., "kafka-s3-uploader")

1. **User Creation & Permissions:**
   - Create a new IAM user.
   - Go to the user page, click on **"Add permissions"**, and choose **"Create inline policy"**.
   - Switch the policy editor to **JSON**.
   - Copy the contents from `amazon_s3_bucker_user_policy.json` and paste them into the policy editor.
   - Replace `"your bucket name"` with the name of your S3 bucket.
   - Click **"Next"** and then **"Create policy"**

2. **Obtain Access Credentials:**
   - On the IAM user page, click **"Create access key"** in the Summary section.
   - Choose **"Local Code"** and provide a description.
   - Download the CSV file containing the **Access Key** and **Secret Key**.
   - Add these credentials to your `.env` file, replacing `<your access key>` with your access key and `<your secret>` with your secret key:
     ```env
     AWS_ACCESS_KEY_ID=<your access key>
     AWS_SECRET_ACCESS_KEY=<your secret>
     ```

---

### 3. Python Environment Setup

1. **Create a Virtual Environment:**
   - On Windows:
     ```bash
     python -m venv venv
     venv\scripts\activate
     ```
   - On macOS/Linux:
     ```bash
     python3 -m venv venv
     source venv/bin/activate
     ```

2. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt

