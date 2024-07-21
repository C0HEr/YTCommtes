#!/bin/bash

# Install required Python packages
pip3 install -r requirements.txt

# Start Docker services
docker-compose up -d

# Run Kafka producer to fetch YouTube comments and send to Kafka
python3 app/kafka.py

# Run Spark application to process data from Kafka
python3 app/spark.py &

# Run Streamlit dashboard
streamlit run dashboard.py
