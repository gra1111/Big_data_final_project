# Big-Data-Trading-Final-Project

Proyecto final de la asignatura **Tecnologías de Procesamiento Big Data** (iMAT, ICAI).

Pipeline de análisis de la criptomoneda **DOGEUSD** sobre AWS, dividido en dos flujos:

- **Histórico**: ingesta desde TradingView → S3 (CSV) → Spark/Glue → arquitectura medallion (bronce/plata/oro) con indicadores técnicos (SMA, EMA, RSI, MACD) → visualización en QuickSight.
- **Tiempo real**: datos de Binance → Kafka → Spark Structured Streaming (VWAP por ventanas de 5 min) → Amazon Timestream → Grafana.

## Estructura del repositorio

```
src/
├── data_ingestion/        # Descarga histórica de TradingView y subida a S3
├── medallion/             # Bronce → Plata → Oro + indicadores técnicos (jobs Spark/Glue)
├── realtime_ingestion/    # Producer Kafka con datos de Binance en tiempo real
├── realtime_processing/   # Spark Structured Streaming (cálculo de VWAP)
└── realtime_storage/      # Consumer Kafka → Amazon Timestream
```

```
data/                      # CSVs históricos descargados de TradingView
DOGEUSD/                   # Capa bronce local (CSV particionado por year/month)
DOGEUSD_silver/            # Capa plata local (parquet particionado por year/month)
```

## Requisitos

- Python 3.12
- [`uv`](https://docs.astral.sh/uv/) para gestionar el entorno
- Credenciales AWS configuradas (para los jobs Glue y subida a S3)
- Acceso al clúster Kafka del curso (ver `src/realtime_ingestion/README_KAFKA_COMANDOS.md`)

## Instalación

```bash
uv sync
```

## Configuración (variables de entorno)

Copia la plantilla y rellena tus valores:

```bash
cp .env.example .env
# edita .env con tu broker y tus credenciales
```

| Variable | Descripción |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Host y puerto del broker Kafka (`host:9092`) |
| `KAFKA_USERNAME` | Usuario SASL/PLAIN |
| `KAFKA_PASSWORD` | Contraseña SASL/PLAIN |
| `AWS_REGION` | Región de AWS para Glue/Timestream |

## Uso rápido

```bash
# Producer: publica en Kafka los ticks en tiempo real de Binance
uv run src/realtime_ingestion/real_time_kafka.py

# Consumer streaming: calcula VWAP y publica en el topic de salida
uv run src/realtime_processing/spark_streaming_node.py

# Consumer Timestream: guarda los mensajes de Kafka en Amazon Timestream
uv run src/realtime_storage/timestream.py
```

Los notebooks (`.ipynb`) en `src/data_ingestion/` y `src/medallion/` se ejecutan con Jupyter para la parte histórica.
