from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, coalesce, max, to_date
from datetime import datetime, timedelta
from logs import init_logs_df, process_with_logging
from dotenv import load_dotenv
import os

def init_spark():
    return SparkSession.builder \
        .appName("AccountTurnoverETL") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

def clear_target_parquet(spark, base_path):
    target_path = f"{base_path}/dm_account_turnover_f"
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        path = spark._jvm.org.apache.hadoop.fs.Path(target_path)
        
        if fs.exists(path):
            fs.delete(path, True)
    except Exception as e:
        raise

def fill_account_turnover(spark, base_path, on_date):
    try:
        posting_df = spark.read.parquet(f"{base_path}/ft_posting") \
            .filter(col("oper_date") == to_date(lit(on_date), "yyyy-MM-dd"))
        
        account_df = spark.read.parquet(f"{base_path}/md_account")
        exchange_df = spark.read.parquet(f"{base_path}/exchange_rate") \
            .filter((col("data_actual_date") <= to_date(lit(on_date), "yyyy-MM-dd")) & 
                   (col("data_actual_end_date") >= to_date(lit(on_date), "yyyy-MM-dd")))
        
        credit_ops = posting_df \
            .join(account_df.hint("broadcast"), 
                  posting_df["credit_account_rk"] == account_df["account_rk"], "left") \
            .join(exchange_df.hint("broadcast"), 
                  account_df["currency_rk"] == exchange_df["currency_rk"], "left") \
            .select(
                col("credit_account_rk").alias("account_rk"),
                col("credit_amount"),
                lit(0).alias("debet_amount"),
                coalesce(col("reduced_cource"), lit(1.0)).alias("exchange_rate")
            )
        
        debit_ops = posting_df \
            .join(account_df.hint("broadcast"), 
                  posting_df["debet_account_rk"] == account_df["account_rk"], "left") \
            .join(exchange_df.hint("broadcast"), 
                  account_df["currency_rk"] == exchange_df["currency_rk"], "left") \
            .select(
                col("debet_account_rk").alias("account_rk"),
                lit(0).alias("credit_amount"),
                col("debet_amount"),
                coalesce(col("reduced_cource"), lit(1.0)).alias("exchange_rate")
            )
        
        result_df = credit_ops.union(debit_ops) \
            .groupBy("account_rk") \
            .agg(
                sum(coalesce(col("credit_amount"), lit(0.0))).alias("credit_amount"),
                (sum(coalesce(col("credit_amount"), lit(0.0))) * 
                 coalesce(max("exchange_rate"), lit(1.0))).alias("credit_amount_rub"),
                sum(coalesce(col("debet_amount"), lit(0.0))).alias("debet_amount"),
                (sum(coalesce(col("debet_amount"), lit(0.0))) * 
                 coalesce(max("exchange_rate"), lit(1.0))).alias("debet_amount_rub")
            ) \
            .withColumn("on_date", to_date(lit(on_date), "yyyy-MM-dd"))
        

        result_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .parquet(f"{base_path}/dm_account_turnover_f")
            
        return result_df
        
    except Exception as e:
        print(f"Ошибка при обработке {on_date}: {str(e)}")
        raise

def process_date_range(spark, base_path, start_date, end_date, logs_df):
    clear_target_parquet(spark, base_path)
    
    current_date = start_date
    while current_date <= end_date:
        process_name = f"fill_account_turnover-{current_date.strftime('%Y-%m-%d')}"
        
        try:
            _, logs_df = process_with_logging(
                spark=spark,
                process_func=lambda s: fill_account_turnover(s, base_path, current_date.strftime('%Y-%m-%d')),
                process_name=process_name,
                logs_df=logs_df
            )
        except Exception as e:
            print(f"Ошибка при обработке даты {current_date}: {str(e)}")
            
        current_date += timedelta(days=1)
    
    return logs_df

if __name__ == "__main__":
    spark = None
    load_dotenv('file.env')
    try:
        spark = init_spark()
        logs_df = init_logs_df(spark)
        
        base_path = os.getenv('PARQUET_PATH')
        logs = os.getenv('LOGS')
        
        logs_df = process_date_range(
            spark=spark,
            base_path=base_path,
            start_date=datetime(2018, 1, 1),
            end_date=datetime(2018, 1, 31),
            logs_df=logs_df
        )
        
        logs_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .parquet(f"{logs}/etl_logs")
            
        logs_df.show(50, truncate=False)
        
    finally:
        if spark:
            spark.stop()