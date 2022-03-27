from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

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
    spark.sparkContext.setLogLevel("ERROR")
    jsonschema = StructType().add("$schema", StringType())\
            .add("id", StringType())\
            .add("namespace", StringType())\
            .add("title", StringType())\
            .add("comment", StringType())\
            .add("timestamp", TimestampType())\
            .add("user", StringType())\
            .add("bot", BooleanType())\
            .add("minor", BooleanType())\
            .add("patrolled", BooleanType())\
            .add("server_url", StringType())\
            .add("server_name", StringType())\
            .add("server_script_path", StringType())\
            .add("wiki", StringType())\
            .add("parsedcomment", StringType())\
            .add("meta_domain", StringType())\
            .add("meta_uri", StringType())\
            .add("meta_request_id", StringType())\
            .add("meta_stream", StringType())\
            .add("meta_topic", StringType())\
            .add("meta_dt", DateType())\
            .add("meta_partition", IntegerType())\
            .add("meta_offset", IntegerType())\
            .add("meta_id", StringType())\
            .add("length_old", IntegerType())\
            .add("length_new", IntegerType())\
            .add("revision_old", IntegerType())\
            .add("revision_new", IntegerType())

    print('initializing streaming...')
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "recentchanges") \
      .load()

    print('connected to kafka')
    parsed = df.select(
        from_json(col("value").cast("string"),
                  jsonschema).alias("parsed_value"))\
                          .select("parsed_value.*")
    print('select expression')
    parsed.printSchema()

    # get global changes by the minute
    global_edits = parsed\
            .groupBy(window("timestamp", "1 minute"))\
            .count()

    # for german wikipedia filter by server_name
    german_edits = parsed\
            .filter(parsed.server_name.rlike(r"de\..*"))\
            .groupBy(window("timestamp", "1 minute"))\
            .count()

    query = german_edits.writeStream\
           .format("console")\
           .outputMode("complete")\
           .start()

    query.awaitTermination()
    spark.stop()
