# -*- coding: utf-8 -*-
import os
import json
import time
from datetime import datetime, timezone
import boto3
from kafka import KafkaConsumer

# kafka config (leída de variables de entorno; ver .env.example)
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
USERNAME = os.environ.get("KAFKA_USERNAME", "kafka_client")
PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
GROUP_ID = "imat3b_timestream_writer"

TOPIC_RAW = "imat3b-DOGE"
TOPIC_VWAP = "imat3b-DOGE-VWAP"

# timestream config
REGION = "eu-west-1"
DATABASE = "imat3a_crypto_rt"
QUOTES_TABLE = "imat3b-DOGE"
VWAP_TABLE = "imat3b-DOGE-VWAP"


def now_epoch_ms() -> str:
    return str(int(datetime.now(timezone.utc).timestamp() * 1000))


def iso_to_epoch_ms(value: str) -> str:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return str(int(dt.timestamp() * 1000))


def main() -> None:
    profile = "ReadOnlyAccess-911167893020"
    session = boto3.Session(profile_name=profile)
    ts_client = session.client("timestream-write", region_name=REGION)
    print(f"Cliente Timestream conectado a la región {REGION}")

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    # Suscribirse a ambos topicos
    consumer.subscribe([TOPIC_RAW, TOPIC_VWAP])
    print(f"Escuchando tópicos: {TOPIC_RAW}, {TOPIC_VWAP}")

    try:
        for msg in consumer:
            topic = msg.topic
            data = msg.value

            # logica para el topico de datos crudos
            if topic == TOPIC_RAW:
                event_time_ms = iso_to_epoch_ms(data["@timestamp"])
                version_ms = int(time.time() * 1000)

                record = {
                    "Dimensions": [
                        {"Name": "symbol", "Value": str(data["symbol"])},
                        {"Name": "source_topic", "Value": str(TOPIC_RAW)}
                    ],
                    "MeasureName": "market_metrics",
                    "MeasureValues": [
                        {
                            "Name": "close_price",
                            "Value": str(float(data["close"])),
                            "Type": "DOUBLE"
                        },
                        {
                            "Name": "trade_volume",
                            "Value": str(float(data["volume"])),
                            "Type": "DOUBLE"
                        }
                    ],
                    "MeasureValueType": "MULTI",
                    "Time": event_time_ms,
                    "TimeUnit": "MILLISECONDS",
                    "Version": version_ms
                }

                ts_client.write_records(
                    DatabaseName=DATABASE,
                    TableName=QUOTES_TABLE,
                    Records=[record]
                )

                print(
                    f"Insertados close_price y trade_volume de {data['symbol']} "
                    f"a las {data['@timestamp']}"
                )

            # lógica para el tópico de vwap
            elif topic == TOPIC_VWAP:
                event_time_ms = iso_to_epoch_ms(data["window_end"])
                version_ms = int(time.time() * 1000)

                record = {
                    "Dimensions": [
                        {"Name": "symbol", "Value": str(data["symbol"])},
                        {"Name": "window_start", "Value": str(data["window_start"])},
                        {"Name": "window_end", "Value": str(data["window_end"])},
                        {"Name": "source_topic", "Value": str(TOPIC_VWAP)}
                    ],
                    "MeasureName": "aggregated_metrics",
                    "MeasureValues": [
                        {
                            "Name": "vwap_value",
                            "Value": str(float(data["vwap"])),
                            "Type": "DOUBLE"
                        }
                    ],
                    "MeasureValueType": "MULTI",
                    # usamos el fin de la ventana como timestamp del evento
                    "Time": event_time_ms,
                    "TimeUnit": "MILLISECONDS",
                    "Version": version_ms
                }

                ts_client.write_records(
                    DatabaseName=DATABASE,
                    TableName=VWAP_TABLE,
                    Records=[record]
                )

                print(
                    f"Insertado vwap_value de {data['symbol']} "
                    f"para ventana {data['window_start']} -> {data['window_end']}"
                )

    except KeyboardInterrupt:
        print("Deteniendo consumidor de Kafka...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
