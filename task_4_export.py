from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
from logs import init_logs_df, process_with_logging
from dotenv import load_dotenv
import os

def init_spark():
    return SparkSession.builder \
        .appName("Export") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

def export_f101_to_csv(spark, base_path, logs_df=None):
    def export_func(spark):
        df = spark.read.parquet(f"{base_path}/dm_f101_round_f")
        
        csv_file_path = f"{base_path}/dm_f101_round_f.csv"
        
        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)
        
        df.toPandas().to_csv(csv_file_path, index=False)
        
        return df
    
    return process_with_logging(
        spark=spark,
        process_func=export_func,
        process_name="export_f101_to_csv",
        logs_df=logs_df
    )

if __name__ == "__main__":
    load_dotenv('file.env')
    spark = None
    try:
        spark = init_spark()
        logs_df = init_logs_df(spark)
        
        base_path = os.getenv('PARQUET_PATH', '/home/jovyan/parquet_data')
        
        if not os.path.exists(base_path):
            raise FileNotFoundError(f"Директория не существует: {base_path}")
        
        result_df, logs_df = export_f101_to_csv(
            spark=spark,
            base_path=base_path,
            logs_df=logs_df
        )
        
        logs_path = f"{base_path}/etl_logs"
        logs_df.write.mode("append").parquet(logs_path)

        print("Логи сохранены в:", logs_path)
        
    finally:
        if spark:
            spark.stop()