from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, row_number, lpad
from pyspark.sql.types import StringType, FloatType
from datetime import datetime
from pyspark.sql.window import Window
from dotenv import load_dotenv
from logs import init_logs_df, process_with_logging
import os

def init_spark():
    return SparkSession.builder.appName("DataProcess").getOrCreate()

def process_balance(spark, input_path, output_path):
    df = spark.read.options(delimiter=";", header=True, inferSchema=True).csv(input_path)
    
    df = (df
          .withColumn("ON_DATE", to_date(col("ON_DATE"), "dd.MM.yyyy"))
          .withColumn("BALANCE_OUT", col("BALANCE_OUT").cast("decimal(18,2)"))
          .filter(col("on_date").isNotNull() & col("account_rk").isNotNull())
          .select("on_date", "account_rk", "currency_rk", "balance_out")
          .distinct()
         )
    
    window = Window.partitionBy("on_date", "account_rk").orderBy("currency_rk", "balance_out")
    df = df.withColumn("row_num", row_number().over(window)).filter(col("row_num") == 1).drop("row_num")
    
    df.write.mode("overwrite").parquet(output_path)
    #df.show(50)
    return df

def process_posting(spark, input_path, output_path):
    df = spark.read.options(delimiter=";", header=True, inferSchema=True).csv(input_path)
    
    df = (df
          .withColumn("credit_amount", col("CREDIT_AMOUNT").cast(FloatType()))
          .withColumn("debet_amount", col("DEBET_AMOUNT").cast(FloatType()))
          .withColumn("oper_date", to_date(col("OPER_DATE"), "dd-MM-yyyy"))
          .filter(col("credit_account_rk").isNotNull() & col("debet_account_rk").isNotNull())
          .select("oper_date", "credit_account_rk", "debet_account_rk", "credit_amount", "debet_amount")
          .distinct()
         )
    
    df.write.mode("overwrite").parquet(output_path)
    #df.show(50)
    return df

def process_account(spark, input_path, output_path):
    df = spark.read.options(delimiter=";", header=True, inferSchema=True).csv(input_path)
    
    df = (df
          .withColumn("account_number", col("ACCOUNT_NUMBER").cast(StringType()))
          .withColumn("currency_code", col("CURRENCY_CODE").cast(StringType()))
          .withColumn("data_actual_date", to_date(col("DATA_ACTUAL_DATE"), "yyyy.MM.dd"))
          .withColumn("data_actual_end_date", to_date(col("DATA_ACTUAL_END_DATE"), "yyyy.MM.dd"))
          .filter(col("data_actual_date").isNotNull() & 
                 col("data_actual_end_date").isNotNull() &
                 col("account_rk").isNotNull())
          .select("data_actual_date", "data_actual_end_date", "account_rk", 
                 "account_number", "char_type", "currency_rk", "currency_code")
          .distinct()
         )
    
    window = Window.partitionBy("data_actual_date", "account_rk").orderBy("account_number", "currency_code")
    df = df.withColumn("row_num", row_number().over(window)).filter(col("row_num") == 1).drop("row_num")
    
    df.write.mode("overwrite").parquet(output_path)
    #df.show(50)
    return df

def process_currency(spark, input_path, output_path):
    df = spark.read.options(delimiter=";", header=True, inferSchema=True, encoding="windows-1252").csv(input_path)
    
    df = (df
          .withColumn("data_actual_date", to_date(col("data_actual_date"), "yyyy.MM.dd"))
          .withColumn("data_actual_end_date", to_date(col("data_actual_end_date"), "yyyy.MM.dd"))
          .withColumn("currency_code", col("CURRENCY_CODE").cast(StringType()))
          .withColumn("currency_code", lpad(col("currency_code"), 3, "0"))
          .filter(col("data_actual_date").isNotNull() & col("currency_rk").isNotNull())
          .select("currency_rk", "data_actual_date", "data_actual_end_date", 
                 "currency_code", "code_iso_char")
          .distinct()
         )
    
    window = Window.partitionBy("currency_rk", "data_actual_date").orderBy("currency_code", "code_iso_char")
    df = df.withColumn("row_num", row_number().over(window)).filter(col("row_num") == 1).drop("row_num")
    
    df.write.mode("overwrite").parquet(output_path)
    #df.show(50)
    return df

def process_exchange_rate(spark, input_path, output_path):
    df = spark.read.options(delimiter=";", header=True, inferSchema=True).csv(input_path)
    
    df = (df
          .withColumn("code_iso_num", col("CODE_ISO_NUM").cast(StringType()))
          .withColumn("reduced_cource", col("REDUCED_COURCE").cast(FloatType()))
          .withColumn("data_actual_date", to_date(col("data_actual_date"), "yyyy.MM.dd"))
          .withColumn("data_actual_end_date", to_date(col("data_actual_end_date"), "yyyy.MM.dd"))
          .filter(col("data_actual_date").isNotNull() & col("currency_rk").isNotNull())
          .select("data_actual_date", "data_actual_end_date", "currency_rk", 
                 "reduced_cource", "code_iso_num")
          .distinct()
         )
    
    window = Window.partitionBy("currency_rk", "data_actual_date").orderBy("reduced_cource", "code_iso_num")
    df = df.withColumn("row_num", row_number().over(window)).filter(col("row_num") == 1).drop("row_num")
    
    df.write.mode("overwrite").parquet(output_path)
    #df.show(50)
    return df

def process_ledger_account(spark, input_path, output_path):
    df = spark.read.options(delimiter=";", header=True, inferSchema=True).csv(input_path)
    
    df = (df
          .withColumn("start_date", to_date(col("START_DATE"), "yyyy.MM.dd"))
          .withColumn("end_date", to_date(col("END_DATE"), "yyyy.MM.dd"))
          .filter(col("ledger_account").isNotNull() & col("start_date").isNotNull())
         )
    
    window = Window.partitionBy("ledger_account", "start_date").orderBy("chapter", "section_number", "ledger1_account")
    df = df.withColumn("row_num", row_number().over(window)).filter(col("row_num") == 1).drop("row_num")
    
    df.write.mode("overwrite").parquet(output_path)
    #df.show(50)
    return df

def main():
    load_dotenv('file.env')
    input_balance_path = os.getenv('BALANCE_INPUT_FILE')
    input_posting_path = os.getenv('POSTING_INPUT_FILE')
    input_account_path = os.getenv('ACCOUNT_INPUT_FILE')
    input_currency_path = os.getenv('CURRENCY_INPUT_FILE')
    input_exchange_rate_path = os.getenv('EXCHANGE_RATE_INPUT_FILE')
    input_ledger_account_path = os.getenv('LEDGER_ACCOUNT_INPUT_FILE')
    
    output_balance_path = os.getenv('BALANCE_OUTPUT_PATH')
    output_posting_path = os.getenv('POSTING_OUTPUT_PATH')
    output_account_path = os.getenv('ACCOUNT_OUTPUT_PATH')
    output_currency_path = os.getenv('CURRENCY_OUTPUT_PATH')
    output_exchange_rate_path = os.getenv('EXCHANGE_RATE_OUTPUT_PATH')
    output_ledger_account_path = os.getenv('LEDGER_ACCOUNT_OUTPUT_PATH')

    logs = os.getenv('LOGS')
    
    spark = init_spark()
    logs_df = init_logs_df(spark)

    balance_df, logs_df = process_with_logging(
        spark, 
        lambda s: process_balance(s, input_balance_path, output_balance_path),
        "Process Balance", 
        logs_df
    )
    
    posting_df, logs_df = process_with_logging(
        spark,
        lambda s: process_posting(s, input_posting_path, output_posting_path),
        "Process Posting",
        logs_df
    )

    account_df, logs_df = process_with_logging(
        spark, 
        lambda s: process_account(s, input_account_path, output_account_path),
        "Process Account", 
        logs_df
    )
    
    currency_df, logs_df = process_with_logging(
        spark,
        lambda s: process_currency(s, input_currency_path, output_currency_path),
        "Process Currency",
        logs_df
    )

    exchange_rate_df, logs_df = process_with_logging(
        spark, 
        lambda s: process_exchange_rate(s, input_exchange_rate_path, output_exchange_rate_path),
        "Process Exchange_rate", 
        logs_df
    )
    
    ledger_account_df, logs_df = process_with_logging(
        spark,
        lambda s: process_ledger_account(s, input_ledger_account_path, output_ledger_account_path),
        "Process Ledger_account",
        logs_df
    )

    logs_df.write.mode("append").parquet(f"{logs}")
    logs_df.show(truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()