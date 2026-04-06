import sys
import time
import psutil
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

def run_experiment(optimize=False):
    # 1. Создаем Spark-сессию
    spark = SparkSession.builder \
        .appName(f"Lab2_Kickstarter_{'Opt' if optimize else 'Base'}") \
        .master("local[*]") \
        .getOrCreate()
    
    # Убираем WARN и FATAL. Оставляем только критические ошибки.
    spark.sparkContext.setLogLevel("ERROR")

    print(f"\n--- Старт эксперимента: {'ОПТИМИЗИРОВАННЫЙ' if optimize else 'БАЗОВЫЙ'} ---")
    
    # Начинаем замер времени
    start_time = time.time()

    # 2. Чтение данных из HDFS
    df = spark.read.csv("hdfs://localhost:9000/lab2/dataset.csv", header=True, inferSchema=True)

    # 3. Блок оптимизации
    if optimize:
        # Увеличиваем параллелизм и кэшируем данные в оперативной памяти
        df = df.repartition(4).cache()

    # 4. Обработка данных
    # Ищем самые собираемые категории среди успешных проектов
    result = df.filter(col('state') == 'successful') \
               .groupBy('main_category') \
               .agg({'usd_pledged_real': 'sum', 'backers': 'avg'}) \
               .orderBy(desc('sum(usd_pledged_real)'))

    # 5. Запуск вычислений
    result.show(5)

    # Начинаем замер памяти
    end_time = time.time()
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / (1024 * 1024)

    # Вывод результатов для отчета
    print("\n[РЕЗУЛЬТАТЫ]")
    print(f"Время выполнения: {end_time - start_time:.2f} сек")
    print(f"Использовано RAM (Драйвер): {memory_mb:.2f} MB\n")

    # логируем Jobs и Stages
    print("У Вас есть 100 секунд, чтобы открыть Spark UI и сделать скриншоты.")
    print("Перейдите в браузере по адресу: http://localhost:4040")
    time.sleep(100) 
    
    spark.stop()

if __name__ == "__main__":
    # Проверяем, передан ли флаг --opt при запуске скрипта
    is_opt = len(sys.argv) > 1 and sys.argv[1] == "--opt"
    run_experiment(optimize=is_opt)