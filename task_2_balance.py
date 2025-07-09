from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, to_date, when
from datetime import datetime, timedelta
from logs import init_logs_df, process_with_logging
from dotenv import load_dotenv
import os

def init_spark():
    return SparkSession.builder \
        .appName("AccountBalanceETL") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

def clear_target_parquet(spark, base_path):
    target_path = f"{base_path}/dm_account_balance_f"
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        path = spark._jvm.org.apache.hadoop.fs.Path(target_path)
        if fs.exists(path):
            fs.delete(path, True)
    except Exception as e:
        print(f"Ошибка при очистке целевого паркета: {str(e)}")
        raise

def load_initial_balance(spark, base_path, initial_date):
    try:
        initial_date_dt = to_date(lit(initial_date.strftime('%Y-%m-%d')), "yyyy-MM-dd")
        
        balance_df = spark.read.parquet(f"{base_path}/ft_balance") \
            .filter(col("on_date") == initial_date_dt)
        
        exchange_rate_df = spark.read.parquet(f"{base_path}/exchange_rate") \
            .filter(
                (col("data_actual_date") <= initial_date_dt) & 
                (col("data_actual_end_date") >= initial_date_dt)
            )
        
        result_df = balance_df.join(
            exchange_rate_df.select(
                col("currency_rk"),
                col("reduced_cource")
            ),
            "currency_rk",
            "left"
        ).select(
            initial_date_dt.alias("on_date"),
            col("account_rk"),
            col("balance_out"),
            (col("balance_out") * coalesce(col("reduced_cource"), lit(1.0))).alias("balance_out_rub")
        )
        
        result_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .parquet(f"{base_path}/dm_account_balance_f")
        result_df.show(114)
        return result_df
        
    except Exception as e:
        print(f"Ошибка при загрузке начального баланса на дату {initial_date}: {str(e)}")
        raise

def fill_account_balance(spark, base_path, on_date):
    try:
        on_date_dt = to_date(lit(on_date.strftime('%Y-%m-%d')), "yyyy-MM-dd")
        
        if on_date == datetime(2017, 12, 31).date():
            return load_initial_balance(spark, base_path, on_date)
        
        account_df = spark.read.parquet(f"{base_path}/md_account")
        turnover_df = spark.read.parquet(f"{base_path}/dm_account_turnover_f") \
            .filter(col("on_date") == on_date_dt)
        
        prev_date = on_date - timedelta(days=1)
        prev_date_dt = to_date(lit(prev_date.strftime('%Y-%m-%d')), "yyyy-MM-dd")
        
        try:
            prev_balance_df = spark.read.parquet(f"{base_path}/dm_account_balance_f") \
                .filter(col("on_date") == prev_date_dt)
        except:
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
            schema = StructType([
                StructField("account_rk", StringType(), True),
                StructField("balance_out", DoubleType(), True),
                StructField("balance_out_rub", DoubleType(), True),
                StructField("on_date", DateType(), True)
            ])
            prev_balance_df = spark.createDataFrame([], schema)
        
        def calculate_balance(accounts, is_active):
            if is_active:
                balance_expr = (
                    coalesce(col("prev_balance_out"), lit(0.0)) + 
                    coalesce(col("debet_amount"), lit(0.0)) - 
                    coalesce(col("credit_amount"), lit(0.0))
                )
                rub_balance_expr = (
                    coalesce(col("prev_balance_out_rub"), lit(0.0)) + 
                    coalesce(col("debet_amount_rub"), lit(0.0)) - 
                    coalesce(col("credit_amount_rub"), lit(0.0))
                )
            else:
                balance_expr = (
                    coalesce(col("prev_balance_out"), lit(0.0)) - 
                    coalesce(col("debet_amount"), lit(0.0)) + 
                    coalesce(col("credit_amount"), lit(0.0))
                )
                rub_balance_expr = (
                    coalesce(col("prev_balance_out_rub"), lit(0.0)) - 
                    coalesce(col("debet_amount_rub"), lit(0.0)) + 
                    coalesce(col("credit_amount_rub"), lit(0.0))
                )
    
            return accounts.join(
                prev_balance_df.select(
                    col("account_rk").alias("prev_account_rk"),
                    col("balance_out").alias("prev_balance_out"),
                    col("balance_out_rub").alias("prev_balance_out_rub")
                ),
                col("account_rk") == col("prev_account_rk"),
                "left"
            ).join(
                turnover_df.select(
                    col("account_rk").alias("turnover_account_rk"),
                    col("debet_amount"),
                    col("debet_amount_rub"),
                    col("credit_amount"),
                    col("credit_amount_rub")
                ),
                col("account_rk") == col("turnover_account_rk"),
                "left"
            ).select(
                on_date_dt.alias("on_date"),
                col("account_rk"),
                balance_expr.alias("balance_out"),
                rub_balance_expr.alias("balance_out_rub")
            ).filter(
                (on_date_dt >= col("data_actual_date")) & 
                (on_date_dt <= col("data_actual_end_date"))
            )
        
        active_balance = calculate_balance(
            account_df.filter(col("char_type") == 'А'), 
            is_active=True
        )
        passive_balance = calculate_balance(
            account_df.filter(col("char_type") == 'П'), 
            is_active=False
        )
        
        result_df = active_balance.union(passive_balance)
        
        result_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .parquet(f"{base_path}/dm_account_balance_f")
        result_df.show(114)
        return result_df
        
    except Exception as e:
        print(f"Ошибка при расчете баланса на дату {on_date}: {str(e)}")
        raise

def process_balance_date_range(spark, base_path, start_date, end_date, logs_df):
    clear_target_parquet(spark, base_path)
    
    current_date = start_date
    while current_date <= end_date:
        process_name = f"fill_account_balance-{current_date.strftime('%Y-%m-%d')}"
        
        try:
            _, logs_df = process_with_logging(
                spark=spark,
                process_func=lambda s: fill_account_balance(s, base_path, current_date),
                process_name=process_name,
                logs_df=logs_df
            )
        except Exception as e:
            print(f"Ошибка при обработке даты {current_date}: {str(e)}")
            
        current_date += timedelta(days=1)
    
    return logs_df

if __name__ == "__main__":
    spark = None
    try:
        load_dotenv('file.env')
        spark = init_spark()
        logs_df = init_logs_df(spark)
        
        base_path = os.getenv('PARQUET_PATH')
        
        logs_df = process_balance_date_range(
            spark=spark,
            base_path=base_path,
            start_date=datetime(2017, 12, 31).date(),
            end_date=datetime(2018, 1, 31).date(),
            logs_df=logs_df
        )
        
        logs_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .parquet(f"{base_path}/etl_logs")
            
        logs_df.show(50, truncate=False)
        
    finally:
        if spark:
            spark.stop()