import json
import time
from kafka import KafkaConsumer
import pandas as pd
import psycopg2

# PostgreSQL Database Connection Configuration
DB_CONFIG = {
    "dbname": "streaming_analytics_db",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432
}

def init_db():
    """Initialize PostgreSQL analytics table if it doesn't exist."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_aggregates (
            event_id VARCHAR(255) PRIMARY KEY,
            device_id VARCHAR(255),
            event_type VARCHAR(255),
            metric_value DOUBLE PRECISION,
            timestamp TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ PostgreSQL telemetry table initialized successfully.")

def consume_events():
    init_db()
    
    # Initialize Kafka Consumer
    consumer = KafkaConsumer(
        'telemetry-events',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='analytics-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("🚀 Kafka Consumer started. Listening for live telemetry events...")
    
    buffer = []
    BATCH_SIZE = 5  # Process in mini-batches for Pandas analytics

    for message in consumer:
        event = message.value
        print(f"📥 Received Event -> ID: {event.get('eventId')} | Device: {event.get('deviceId')} | Value: {event.get('metricValue')}")
        buffer.append(event)

        # When batch size is reached, analyze with Pandas and save to Postgres
        if len(buffer) >= BATCH_SIZE:
            df = pd.DataFrame(buffer)
            mean_val = df['metricValue'].mean()
            print(f"📊 [Analytics Engine] Batch processed {len(df)} events. Average Metric Value: {mean_val:.2f}")
            
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                cur = conn.cursor()
                for _, row in df.iterrows():
                    cur.execute("""
                        INSERT INTO telemetry_aggregates (event_id, device_id, event_type, metric_value, timestamp)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO NOTHING
                    """, (
                        row.get('eventId'), 
                        row.get('deviceId'), 
                        row.get('eventType'), 
                        row.get('metricValue'), 
                        row.get('timestamp')
                    ))
                conn.commit()
                cur.close()
                conn.close()
                print("💾 Batch successfully persisted to PostgreSQL database.")
            except Exception as e:
                print(f"❌ Database insertion error: {e}")
            
            buffer.clear()

if __name__ == "__main__":
    consume_events()
