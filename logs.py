from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from datetime import datetime

def init_logs_schema():
    return StructType([
        StructField("process_name", StringType(), nullable=False),
        StructField("start_time", TimestampType(), nullable=False),
        StructField("end_time", TimestampType(), nullable=True),
        StructField("duration_sec", LongType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("rows_processed", LongType(), nullable=True),
        StructField("error_message", StringType(), nullable=True)
    ])

def init_logs_df(spark):
    return spark.createDataFrame([], schema=init_logs_schema())

def create_log_entry(spark, process_name, status="RUNNING", start_time=None, 
                    end_time=None, duration_sec=None, rows_processed=None, 
                    error_message=None):
    if start_time is None:
        start_time = datetime.now()
    
    log_row = {
        "process_name": process_name,
        "start_time": start_time,
        "end_time": end_time,
        "duration_sec": duration_sec,
        "status": status,
        "rows_processed": rows_processed,
        "error_message": error_message
    }
    
    return spark.createDataFrame([log_row], schema=init_logs_schema())

def update_logs_df(logs_df, process_name, status=None, end_time=None, 
                  duration_sec=None, rows_processed=None, error_message=None):
    if status is not None:
        logs_df = logs_df.withColumn(
            "status",
            when(col("process_name") == process_name, lit(status)).otherwise(col("status")))
    
    if end_time is not None:
        logs_df = logs_df.withColumn(
            "end_time",
            when(col("process_name") == process_name, lit(end_time)).otherwise(col("end_time")))
    
    if duration_sec is not None:
        logs_df = logs_df.withColumn(
            "duration_sec",
            when(col("process_name") == process_name, lit(duration_sec)).otherwise(col("duration_sec")))
    
    if rows_processed is not None:
        logs_df = logs_df.withColumn(
            "rows_processed",
            when(col("process_name") == process_name, lit(rows_processed)).otherwise(col("rows_processed")))
    
    if error_message is not None:
        logs_df = logs_df.withColumn(
            "error_message",
            when(col("process_name") == process_name, lit(error_message)).otherwise(col("error_message")))
    
    return logs_df

def process_with_logging(spark, process_func, process_name, logs_df, *args, **kwargs):
    start_time = datetime.now()
    new_log = create_log_entry(spark, process_name, start_time=start_time)
    logs_df = logs_df.union(new_log)
    
    try:
        result_df = process_func(spark, *args, **kwargs)
        row_count = result_df.count() if result_df else 0
        end_time = datetime.now()
        
        logs_df = update_logs_df(
            logs_df,
            process_name=process_name,
            status="COMPLETED",
            end_time=end_time,
            duration_sec=(end_time - start_time).total_seconds(),
            rows_processed=row_count
        )
        
        return result_df, logs_df
        
    except Exception as e:
        end_time = datetime.now()
        logs_df = update_logs_df(
            logs_df,
            process_name=process_name,
            status="FAILED",
            end_time=end_time,
            duration_sec=(end_time - start_time).total_seconds(),
            error_message=str(e)
        )
        
        return None, logs_df