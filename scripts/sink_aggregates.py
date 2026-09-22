import json
import sqlite3
from kafka import KafkaConsumer

# Connect to the same SQLite database file
db = sqlite3.connect("hospital.db")
cursor = db.cursor()

# Create table for window aggregates if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS ward_alert_counts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ward TEXT,
        window_start REAL,
        window_end REAL,
        abnormal_events INTEGER
    )
""")
db.commit()

# Connect to Kafka consumer for window counts
consumer = KafkaConsumer(
    'ward_alert_counts',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='db-aggregate-sink-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Aggregate sink started, saving window counts to SQLite...")

for message in consumer:
    data = message.value
    
    ward = data.get("ward")
    window_start = data.get("window_start")
    window_end = data.get("window_end")
    abnormal_events = data.get("abnormal_events")

    sql = """
        INSERT INTO ward_alert_counts (ward, window_start, window_end, abnormal_events)
        VALUES (?, ?, ?, ?)
    """
    values = (ward, window_start, window_end, abnormal_events)
    
    cursor.execute(sql, values)
    db.commit()
    
    print(f"Saved window aggregate for {ward} ({window_start} - {window_end}): {abnormal_events} events")