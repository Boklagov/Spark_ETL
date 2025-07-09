from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, lit, round, format_number
from datetime import datetime, timedelta
from logs import init_logs_df, process_with_logging
from dotenv import load_dotenv
import os

def init_spark():
    return SparkSession.builder \
        .appName("F101") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

def clear_target_parquet(spark, base_path):
    target_path = f"{base_path}/dm_f101_round_f"
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        path = spark._jvm.org.apache.hadoop.fs.Path(target_path)
        if fs.exists(path):
            fs.delete(path, True)
    except Exception as e:
        raise

def process_f101_round_report(spark, base_path, start_date, end_date, logs_df=None):

    process_name = f"f101_report_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    
    def process_func(spark):
        clear_target_parquet(spark, base_path)
        
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        prev_date_str = (start_date - timedelta(days=1)).strftime('%Y-%m-%d')

        md_account = spark.read.parquet(f"{base_path}/md_account")
        md_ledger_account = spark.read.parquet(f"{base_path}/ledger_account")
        account_balance = spark.read.parquet(f"{base_path}/dm_account_balance_f")
        account_turnover = spark.read.parquet(f"{base_path}/dm_account_turnover_f")

        account_data = md_account.join(
            md_ledger_account,
            md_account["account_number"].startswith(md_ledger_account["ledger_account"].cast("string")),
            "left"
        ).select(
            col("account_rk"),
            col("currency_code").cast("integer"),
            col("char_type").alias("characteristic"),
            col("chapter"),
            col("ledger_account")
        ).filter(
            (col("data_actual_date") <= end_date_str) &
            (col("data_actual_end_date") >= start_date_str)
        )

        balance_data = account_balance.join(
            account_data,
            "account_rk",
            "inner"
        ).groupBy("chapter", "ledger_account", "characteristic").agg(
            round(sum(when(
                (col("currency_code").isin(810, 643)) & 
                (col("on_date") == prev_date_str),
                col("balance_out_rub")
            ).otherwise(0)), 2).alias("balance_in_rub"),
            round(sum(when(
                (~col("currency_code").isin(810, 643)) & 
                (col("on_date") == prev_date_str),
                col("balance_out_rub")
            ).otherwise(0)), 2).alias("balance_in_val"),
            round(sum(when(
                col("on_date") == prev_date_str,
                col("balance_out_rub")
            ).otherwise(0)), 2).alias("balance_in_total"),
            round(sum(when(
                (col("currency_code").isin(810, 643)) & 
                (col("on_date") == end_date_str),
                col("balance_out_rub")
            ).otherwise(0)), 2).alias("balance_out_rub"),
            round(sum(when(
                (~col("currency_code").isin(810, 643)) & 
                (col("on_date") == end_date_str),
                col("balance_out_rub")
            ).otherwise(0)), 2).alias("balance_out_val"),
            round(sum(when(
                col("on_date") == end_date_str,
                col("balance_out_rub")
            ).otherwise(0)), 2).alias("balance_out_total")
        )

        turnover_data = account_turnover.join(
            account_data,
            "account_rk",
            "left"
        ).filter(
            col("on_date").between(start_date_str, end_date_str)
        ).groupBy("chapter", "ledger_account", "characteristic").agg(
            round(sum(when(
                col("currency_code").isin(810, 643),
                col("debet_amount_rub")
            ).otherwise(0)), 2).alias("turn_deb_rub"),
            round(sum(when(
                (~col("currency_code").isin(810, 643)),
                col("debet_amount_rub")
            ).otherwise(0)), 2).alias("turn_deb_val"),
            round(sum(col("debet_amount_rub")), 2).alias("turn_deb_total"),
            round(sum(when(
                col("currency_code").isin(810, 643),
                col("credit_amount_rub")
            ).otherwise(0)), 2).alias("turn_cre_rub"),
            round(sum(when(
                (~col("currency_code").isin(810, 643)),
                col("credit_amount_rub")
            ).otherwise(0)), 2).alias("turn_cre_val"),
            round(sum(col("credit_amount_rub")), 2).alias("turn_cre_total")
        )

        report = balance_data.join(
            turnover_data,
            ["chapter", "ledger_account", "characteristic"],
            "left"
        ).withColumn("from_date", lit(start_date_str)) \
         .withColumn("to_date", lit(end_date_str)) \
         .select(
             "from_date", "to_date", "chapter", "ledger_account", "characteristic",
             "balance_in_rub", "balance_in_val", "balance_in_total",
             "turn_deb_rub", "turn_deb_val", "turn_deb_total",
             "turn_cre_rub", "turn_cre_val", "turn_cre_total",
             "balance_out_rub", "balance_out_val", "balance_out_total"
         )

        report.write \
            .mode("append") \
            .parquet(f"{base_path}/dm_f101_round_f")

        for column in ["balance_in_rub", "balance_in_val", "balance_in_total",
                      "turn_deb_rub", "turn_deb_val", "turn_deb_total",
                      "turn_cre_rub", "turn_cre_val", "turn_cre_total",
                      "balance_out_rub", "balance_out_val", "balance_out_total"]:
            report = report.withColumn(column, format_number(col(column), 2))

        return report

    return process_with_logging(spark, process_func, process_name, logs_df)

if __name__ == "__main__":
    load_dotenv('file.env')
    spark = None
    try:
        spark = init_spark()
        logs_df = init_logs_df(spark)
        
        base_path = os.getenv('PARQUET_PATH')
        
        start_date = datetime(2018, 1, 1).date()
        end_date = datetime(2018, 1, 31).date()
        
        report_df, logs_df = process_f101_round_report(spark, base_path, start_date, end_date, logs_df)
                   
        #report_df.show(20)
        
        logs_path = f"{base_path}/etl_logs"
        logs_df.write.mode("append").parquet(logs_path)
        #logs_df.show(100)
        
    finally:
        if spark:
            spark.stop()