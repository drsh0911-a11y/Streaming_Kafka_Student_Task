import json
import time
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    'patient_vitals',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='patient-processor-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Processor started, listening for events...")
WATERMARK_DELAY = 20 # 10 seconds event-time watermark

for message in consumer:
    data = message.value
    current_time = time.time()
    event_time = data.get("timestamp", current_time)

    # 1. Data Quality & Watermark Check
    errors = []
    if not data.get("patient_id"):
        errors.append("Missing patient ID")
    if not (30 <= data.get("heart_rate", 0) <= 250):
        errors.append("Invalid heart rate")
    if current_time - event_time > WATERMARK_DELAY:
        errors.append("Event too late for watermark")

    if errors:
        data["quality_errors"] = errors
        producer.send('invalid_patient_vitals', value=data)
        print(f"Rejected event due to: {errors}")
        continue

    # 2. Check for Alerts (e.g., abnormal vitals)
    hr = data["heart_rate"]
    o2 = data["oxygen_level"]
    if hr > 120 or hr < 50 or o2 < 90:
        data["alert_reason"] = f"Abnormal vitals: HR={hr}, O2={o2}"
        producer.send('patient_alerts', value=data)
        print(f"Alert generated for {data['patient_id']}")

        # 3. Window Aggregation Mock
        window_start = int(event_time // 10) * 10
        window_end = window_start + 10
        window_agg = {
            "ward": data["ward"],
            "window_start": window_start,
            "window_end": window_end,
            "abnormal_events": 1
        }
        producer.send('ward_alert_counts', value=window_agg)
        