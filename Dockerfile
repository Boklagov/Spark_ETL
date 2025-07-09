FROM bitnami/spark:3.5

WORKDIR /app

COPY file.env .

CMD ["spark-submit", "task_1.py"]