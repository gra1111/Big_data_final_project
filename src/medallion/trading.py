from pyspark.sql import Window, DataFrame, WindowSpec
import pyspark.sql.functions as F

def sma(df: DataFrame, n: int = 200) -> DataFrame:
    col_name: str = f"sma{n}"
    if col_name in df.columns:
        print(f"Skipping {col_name}: already exists.")
        return df
    window = Window.orderBy("datetime").rowsBetween(-n + 1, 0)
    return df.withColumn(col_name, F.avg("close").over(window))


def ema(df: DataFrame, n: int = 50) -> DataFrame:
    col_name: str = f"ema{n}"
    if col_name in df.columns:
        print(f"Skipping {col_name}: already exists.")
        return df

    alpha: float = 2 / (n + 1)
    window_unbounded = Window.orderBy("datetime").rowsBetween(Window.unboundedPreceding, 0)
    
    return df.withColumn(
        col_name,
        F.aggregate(
            F.collect_list("close").over(window_unbounded),
            F.lit(None).cast("double"),
            lambda acc, x: F.when(acc.isNull(), x).otherwise(acc + alpha * (x - acc))
        )
    )


def rsi(df: DataFrame, n: int = 14) -> DataFrame:
    col_name: str = f"rsi{n}"
    if col_name in df.columns:
        print(f"Skipping {col_name}: already exists.")
        return df

    window_prev = Window.orderBy("datetime")
    window_rolling = Window.orderBy("datetime").rowsBetween(-n + 1, 0)
    df = df.withColumn("diff", F.col("close") - F.lag("close", 1).over(window_prev))
    df = df.withColumn("gain", F.when(F.col("diff") > 0, F.col("diff")).otherwise(0))
    df = df.withColumn("loss", F.when(F.col("diff") < 0, F.abs(F.col("diff"))).otherwise(0))
    df = df.withColumn("avg_gain", F.avg("gain").over(window_rolling))
    df = df.withColumn("avg_loss", F.avg("loss").over(window_rolling))
    df = df.withColumn(
        "rs", 
        F.when(F.col("avg_loss") == 0, F.lit(None))
         .otherwise(F.col("avg_gain") / F.col("avg_loss"))
    )
    df_resultado = df.withColumn(
        col_name, 
        F.when(F.col("avg_loss") == 0, 100.0)
         .otherwise(100 - (100 / (1 + F.col("rs"))))
    )
    
    return df_resultado.drop("diff", "gain", "loss", "avg_gain", "avg_loss", "rs")


def macd(df: DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> DataFrame:
    if "macd_line" in df.columns:
        print("Skipping MACD: already exists.")
        return df
    df = ema(df, n=fast)
    df = ema(df, n=slow)
    
    df = df.withColumn("macd_line", F.col(f"ema{fast}") - F.col(f"ema{slow}"))
    
    df_signal = df.withColumnRenamed("macd_line", "temp_col") \
                  .withColumnRenamed("close", "orig_close") \
                  .withColumnRenamed("temp_col", "close")
    
    df_signal = ema(df_signal, n=signal)
    
    df_resultado = df_signal.withColumnRenamed("close", "macd_line") \
                            .withColumnRenamed(f"ema{signal}", "signal_line") \
                            .withColumnRenamed("orig_close", "close") \
                            .withColumn("macd_histogram", F.col("macd_line") - F.col("signal_line"))
    
    return df_resultado.drop(f"ema{fast}", f"ema{slow}")