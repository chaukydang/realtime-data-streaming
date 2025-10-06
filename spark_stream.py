import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthenticator
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            postcode TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        )
    """)
    
    print("Table created successfully!")
    
def insert_data(session, **kwargs):
    print("inserting data...")
    
    user_id = kwargs.get('id')
    first_name = kwargs.get("first_name")
    last_name = kwargs.get("last_name")
    gender = kwargs.get("gender")
    address = kwargs.get("address")
    postcode = kwargs.get("postcode")
    email = kwargs.get("email")
    username = kwargs.get("username")
    registered_date = kwargs.get("registered_date")
    phone = kwargs.get("phone")
    picture = kwargs.get("picture")
    
    try: 
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                postcode, email, username, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address, 
                postcode, email, username, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")
    except Exception as e: 
        logging.error(f"Couldn't insert data due to {e}")
    
def create_spark_connection():
    s_conn = None
    try: 
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .master('spark://localhost:7077') \
            .config('spark.jars.packages', 
                    "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
                    "org.apache.kafka:kafka-clients:3.6.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()
            
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
    
    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully!")
        return spark_df
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

def create_cassandra_connection():
    
    try:
        # Connect to the cassandra cluster
        cluster = Cluster(['localhost'])
        
        cas_session = cluster.connect()
        
        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None
    
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True)
    ])
    
    sel = spark_df.selectExpr("CAST(value as STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
        
    print(sel)
    return sel

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Create spark connection
    spark_conn = create_spark_connection()
    
    if spark_conn is not None:
        # Connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn) 
        
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)
            session = create_cassandra_connection()
            
            if session is not None:
                create_keyspace(session)
                create_table(session)
                # insert_data(session)
                
                logging.info("Streaming is being started...")
                
                streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                   .option('checkpointLocation', '/tmp/checkpoint')
                                   .option('keyspace', 'spark_streams')
                                   .option('table', 'created_users')
                                   .start()
                                   )
                streaming_query.awaitTermination()