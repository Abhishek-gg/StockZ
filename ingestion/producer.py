import sys
import json
import time
import requests
from loguru import logger
from kafka import KafkaProducer
from kafka.errors import KafkaError

import os
# Add parent directory to path to allow importing config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import settings

def fetch_stock_data(symbol: str):
    """Fetch 1-min intraday stock data from Alpha Vantage."""
    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "1min",
        "apikey": settings.ALPHA_VANTAGE_API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if "Time Series (1min)" not in data:
            logger.warning(f"No time series data returned for {symbol}. Response: {data}")
            return []
            
        time_series = data["Time Series (1min)"]
        records = []
        for ts, values in time_series.items():
            record = {
                "symbol": symbol,
                "timestamp": ts,
                "open": float(values.get("1. open", 0)),
                "high": float(values.get("2. high", 0)),
                "low": float(values.get("3. low", 0)),
                "close": float(values.get("4. close", 0)),
                "volume": int(values.get("5. volume", 0))
            }
            records.append(record)
        return records
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return []

def produce_to_kafka():
    producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    symbols = ["AAPL", "MSFT", "GOOGL"]
    
    logger.info("Starting ingestion loop...")
    try:
        while True:
            for symbol in symbols:
                logger.info(f"Fetching data for {symbol}...")
                records = fetch_stock_data(symbol)
                
                # In a real streaming scenario, we would only push new records.
                # For demo, we just push the latest record available to avoid flooding.
                if records:
                    # Taking the most recent record
                    latest_record = records[0]
                    future = producer.send(
                        settings.KAFKA_TOPIC, 
                        key=symbol,
                        value=latest_record
                    )
                    try:
                        record_metadata = future.get(timeout=10)
                        logger.info(f"Produced message to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                    except KafkaError as e:
                        logger.error(f"Failed to produce message: {e}")
                
                # Alpha Vantage free tier is 25 requests/day. We sleep to avoid hitting limits quickly.
                time.sleep(15) 
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    produce_to_kafka()
