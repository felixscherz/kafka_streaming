from pyspark.sql import SparkSession

import sys
from random import random
from operator import add

if __name__ == '__main__':
    spark = SparkSession.\
        builder.\
        appName('SparkWikipediaStreaming').\
        getOrCreate()
    # Loading data from a JDBC source
    # df = spark.read\
    #     .format("jdbc")\
    #     .option("url", "jdbc:postgresql://postgres:5432/db")\
    #     .option("dbtable", "clients")\
    #     .option("user", "user")\
    #     .option("password", "password")\
    #     .option('driver', 'org.postgresql.Driver')\
    #     .load()
    print('initializing streaming...')
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "recentchanges") \
      .load()

    print('connected to kafka')
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    print('select expression')
    query = df.writeStream\
           .format("console")\
           .start()

    query.awaitTermination()
    spark.stop()
