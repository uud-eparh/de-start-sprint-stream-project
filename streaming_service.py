import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Конфигурация
TOPIC_NAME_IN = 'uud-eparh_in'
TOPIC_NAME_OUT = 'uud-eparh_out'
KAFKA_BOOTSTRAP_SERVERS = 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091'

# Настройки Kafka
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
}

def spark_init(app_name: str) -> SparkSession:
    """Инициализация Spark сессии"""
    spark_jars_packages = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0"
    ])
    
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()

def read_subscribers(spark: SparkSession) -> DataFrame:
    """Чтение подписчиков из PostgreSQL"""
    logger.info("Загрузка данных о подписчиках из PostgreSQL...")
    
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de") \
        .option("dbtable", "subscribers_restaurants") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "student") \
        .option("password", "de-student") \
        .load() \
        .cache()
    
    logger.info(f"Загружено {df.count()} подписчиков")
    return df

def read_campaign_stream(spark: SparkSession) -> DataFrame:
    """Чтение потока акций из Kafka с фильтрацией по времени"""
    schema = StructType([
        StructField("restaurant_id", StringType(), True),
        StructField("adv_campaign_id", StringType(), True),
        StructField("adv_campaign_content", StringType(), True),
        StructField("adv_campaign_owner", StringType(), True),
        StructField("adv_campaign_owner_contact", StringType(), True),
        StructField("adv_campaign_datetime_start", LongType(), True),
        StructField("adv_campaign_datetime_end", LongType(), True),
        StructField("datetime_created", LongType(), True),
    ])
    
    # Читаем поток
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .options(**kafka_security_options) \
        .option("subscribe", TOPIC_NAME_IN) \
        .option("maxOffsetsPerTrigger", 1000) \
        .load() \
        .withColumn("value", f.col("value").cast(StringType())) \
        .withColumn("data", f.from_json(f.col("value"), schema)) \
        .select("data.*")
    
    # Фильтрация по времени
    current_time = f.unix_timestamp(f.current_timestamp())
    filtered_stream = raw_stream.filter(
        (f.col("adv_campaign_datetime_start") <= current_time) &
        (f.col("adv_campaign_datetime_end") >= current_time)
    )
    
    return filtered_stream

def write_to_postgres(df: DataFrame, table_name: str):
    """Запись в PostgreSQL"""
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:postgresql://localhost:5432/de") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .option("user", "jovyan") \
        .option("password", "jovyan") \
        .save()
    logger.info(f"Записано {df.count()} записей в {table_name}")

def write_to_kafka(df: DataFrame, topic: str):
    """Запись в Kafka"""
    kafka_df = df.select(
        f.to_json(f.struct(
            "restaurant_id",
            "adv_campaign_id", 
            "adv_campaign_content",
            "adv_campaign_owner",
            "adv_campaign_owner_contact",
            "adv_campaign_datetime_start",
            "adv_campaign_datetime_end",
            "client_id",
            "datetime_created",
            "trigger_datetime_created"
        )).alias("value")
    )
    
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .options(**kafka_security_options) \
        .option("topic", topic) \
        .mode("append") \
        .save()
    logger.info(f"Отправлено {df.count()} записей в топик {topic}")

def create_foreach_batch_function(subscribers_df: DataFrame):
    """Создает функцию обработки батча с переданным subscribers_df"""
    
    def foreach_batch_function(batch_df: DataFrame, batch_id: int):
        """Обработка микробатча"""
        logger.info(f"Обработка батча {batch_id}, записей: {batch_df.count()}")
        
        if batch_df.rdd.isEmpty():
            logger.warning("Пустой батч, пропускаем")
            return
        
        # JOIN с подписчиками
        joined_df = batch_df.join(
            f.broadcast(subscribers_df),
            on="restaurant_id",
            how="inner"
        ).withColumn(
            "trigger_datetime_created",
            f.unix_timestamp(f.current_timestamp()).cast(IntegerType())
        )
        
        logger.info(f"После JOIN: {joined_df.count()} записей")
        
        if joined_df.count() == 0:
            logger.warning("Нет совпадений по restaurant_id")
            return
        
        # Кэшируем
        joined_df.cache()
        
        # Для PostgreSQL (с полем feedback)
        postgres_df = joined_df.select(
            "restaurant_id",
            "adv_campaign_id", 
            "adv_campaign_content",
            "adv_campaign_owner",
            "adv_campaign_owner_contact",
            "adv_campaign_datetime_start",
            "adv_campaign_datetime_end",
            "datetime_created",
            "client_id",
            "trigger_datetime_created"
        ).withColumn("feedback", f.lit(None).cast(StringType()))
        
        # Отправка в PostgreSQL
        write_to_postgres(postgres_df, "subscribers_feedback")
        
        # Отправка в Kafka
        write_to_kafka(joined_df, TOPIC_NAME_OUT)
        
        # Очистка кэша
        joined_df.unpersist()
        logger.info(f"Батч {batch_id} обработан")
    
    return foreach_batch_function

def main():
    """Основная функция"""
    logger.info("Запуск приложения RestaurantSubscribeStreamingService")
    
    spark = spark_init("RestaurantSubscribeStreamingService")
    
    # Загрузка статических данных
    subscribers_df = read_subscribers(spark)
    
    # Чтение потока
    campaign_stream = read_campaign_stream(spark)
    
    # Создаем функцию обработки с подписчиками
    batch_function = create_foreach_batch_function(subscribers_df)
    
    # Запуск стриминга
    query = campaign_stream.writeStream \
        .foreachBatch(batch_function) \
        .outputMode("append") \
        .trigger(processingTime="15 seconds") \
        .start()
    
    logger.info(f"Стрим запущен. Топик входа: {TOPIC_NAME_IN}, топик выхода: {TOPIC_NAME_OUT}")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
        query.stop()
        logger.info("Стрим остановлен")

if __name__ == "__main__":
    main()