import json
import sqlite3
from kafka import KafkaConsumer

# Connect to a local SQLite database file (creates hospital.db automatically)
db = sqlite3.connect("hospital.db")
cursor = db.cursor()

# Create table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS patient_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        patient_id TEXT,
        ward TEXT,
        heart_rate INTEGER,
        oxygen_level INTEGER,
        alert_reason TEXT,
        timestamp REAL
    )
""")
db.commit()

# Connect to Kafka consumer for alerts
consumer = KafkaConsumer(
    'patient_alerts',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='db-sink-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("SQLite database sink started, saving alerts...")

for message in consumer:
    data = message.value
    
    patient_id = data.get("patient_id")
    ward = data.get("ward")
    heart_rate = data.get("heart_rate")
    oxygen_level = data.get("oxygen_level")
    alert_reason = data.get("alert_reason")
    timestamp = data.get("timestamp")

    sql = """
        INSERT INTO patient_alerts (patient_id, ward, heart_rate, oxygen_level, alert_reason, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    values = (patient_id, ward, heart_rate, oxygen_level, alert_reason, timestamp)
    
    cursor.execute(sql, values)
    db.commit()
    
    print(f"Saved alert for {patient_id} ({ward}) to SQLite database.")