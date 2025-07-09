from pyspark.sql import SparkSession
from logs import init_logs_df, process_with_logging
from dotenv import load_dotenv
import os

def init_spark():
    """Инициализация Spark сессии с оптимизацией для чтения CSV"""
    return SparkSession.builder \
        .appName("Import") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.csv.parser.columnPruning.enabled", "true") \
        .config("spark.sql.csv.parser.inferSchema.enabled", "true") \
        .getOrCreate()

def import_csv_to_parquet(spark, csv_path, output_path, logs_df=None):
    
    def import_func(spark):
        # Чтение CSV с автоматическим определением схемы
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", ",") \
            .option("dateFormat", "yyyy-MM-dd") \
            .csv(csv_path)
        
        if os.path.exists(output_path):
            df.write \
              .mode("overwrite") \
              .parquet(output_path)
        else:
            df.write.parquet(output_path)
        
        return df
    
    return process_with_logging(
        spark=spark,
        process_func=import_func,
        process_name=f"mport",
        logs_df=logs_df
    )

if __name__ == "__main__":
    load_dotenv('file.env')
    spark = None
    try:
        spark = init_spark()
        logs_df = init_logs_df(spark)
        
        base_path = os.getenv('PARQUET_PATH')
        
        csv_path = f"{base_path}/dm_f101_round_f.csv"
        output_path = f"{base_path}/dm_f101_round_f_v2"
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV файл не найден: {csv_path}")

        result_df, logs_df = import_csv_to_parquet(
            spark=spark,
            csv_path=csv_path,
            output_path=output_path,
            logs_df=logs_df
        )
        
        # Сохранение логов
        logs_path = f"{base_path}/etl_logs"
        logs_df.write.mode("append").parquet(logs_path)
        
        
        result_df.printSchema()
        
        result_df.show(20, truncate=False)
        
    finally:
        if spark:
            spark.stop()