import os
from binance import Client
from binance import ThreadedWebsocketManager
import json
from kafka import KafkaProducer
from datetime import datetime, timezone

# Config (leída de variables de entorno; ver .env.example)
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
USERNAME = os.environ.get("KAFKA_USERNAME", "kafka_client")
PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
TOPIC="imat3b-DOGE"
SYMBOL = "DOGEUSDT"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8"),
)
    
def send_message(msg):
    k = msg['k']

    if k['x']:
        key = SYMBOL
        value = {
            "symbol": SYMBOL,
            "@timestamp": str(datetime.fromtimestamp(k['T'] / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
            "close": k['c'],
            'volume': k['v'],
        }

        print("Sent: ", key + " " + str(value))

        producer.send(topic=TOPIC, key=key, value=value)


def main() -> None:    
    twm = ThreadedWebsocketManager()
    twm.start()
    
    twm.start_kline_socket(
    symbol=SYMBOL,
    interval=Client.KLINE_INTERVAL_1MINUTE,
    callback=send_message
    ) 

    input("Press ENTER to exit...\n")
    twm.stop()
    
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()