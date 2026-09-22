import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
   bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

wards = ["Ward-A", "Ward-B", "Ward-C"]

print("Starting patient vitals simulator...")
while True:
    patient_id = f"P-{random.randint(100, 105)}"
    event_id = f"evt-{time.time_ns()}"
    heart_rate = random.randint(50, 140)
    oxygen_level = random.randint(85, 100)
    ward = random.choice(wards)
    
    # Deliberately create some late events (15 seconds in the past) for the bonus/watermark logic
    is_late = random.random() < 0.1
    timestamp = time.time() - (2 if is_late else 0)

    payload = {
        "event_id": event_id,
        "patient_id": patient_id,
        "heart_rate": heart_rate,
        "oxygen_level": oxygen_level,
        "ward": ward,
        "timestamp": timestamp
    }

    producer.send('patient_vitals', key=patient_id, value=payload)
    producer.flush()
    print(f"Sent vitals for {patient_id} in {ward}")
    time.sleep(2)