#!/usr/bin/env python3
# producer/kafka_producer.py

import time
import csv
import json
from kafka import KafkaProducer
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--broker", default="localhost:9093", help="Kafka bootstrap server")
parser.add_argument("--topic", default="smart_meter", help="Kafka topic")
parser.add_argument("--file", default="data/household_power_consumption.txt", help="CSV file")
parser.add_argument("--interval", type=float, default=0.01, help="seconds between messages")
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=[args.broker],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def parse_line(row):
    def safe_float(x):
        # Handle missing values represented as '?' or ''
        if x in ('', '?', None):
            return None
        try:
            return float(x)
        except ValueError:
            return None

    return {
        "Date": row[0],
        "Time": row[1],
        "Global_active_power": safe_float(row[2]),
        "Global_reactive_power": safe_float(row[3]),
        "Voltage": safe_float(row[4]),
        "Global_intensity": safe_float(row[5]),
        "Sub_metering_1": safe_float(row[6]),
        "Sub_metering_2": safe_float(row[7]),
        "Sub_metering_3": safe_float(row[8]),
    }

if __name__ == "__main__":
    if not os.path.exists(args.file):
        raise SystemExit(f"Data file {args.file} not found. Run download_data.sh")
    with open(args.file, "r") as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)  # skip header
        for idx, row in enumerate(reader):
            msg = parse_line(row)
            producer.send(args.topic, msg)
            if idx % 1000 == 0:
                producer.flush()
            time.sleep(args.interval)
