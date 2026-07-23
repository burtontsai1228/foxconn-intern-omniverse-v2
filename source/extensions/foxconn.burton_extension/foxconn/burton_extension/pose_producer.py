import json
import math
import time
from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "robot_pose"

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"sending dummy pose to {BROKER}, topic '{TOPIC}' — Ctrl+C to stop")

t = 0.0
try:
    while True:
        x = 2.0 * math.cos(t)
        y = 2.0 * math.sin(t)
        theta = t % (2 * math.pi)

        msg = {
            "x": round(x, 3),
            "y": round(y, 3),
            "theta": round(theta, 3),
        }
        producer.send(TOPIC, msg)
        print(msg)
        t += 0.05
        time.sleep(0.1)
except KeyboardInterrupt:
    print("\nstopped")
finally:
    producer.flush()
    producer.close()