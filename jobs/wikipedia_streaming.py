from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    # initialize spark session
    spark = SparkSession.\
        builder.\
        appName('SparkWikipediaStreaming').\
        getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # lookup foreign key values for regions
    regions = spark.read\
            .format("jdbc")\
            .option("url", "jdbc:postgresql://postgres:5432/wikipedia_events")\
            .option("dbtable","regions").option("user","user")\
            .option('driver', 'org.postgresql.Driver')\
            .option("password", "password")\
            .load()

    german_id = regions.filter(
        regions.region == 'germany').first()["region_id"]
    global_id = regions.filter(regions.region == 'global').first()["region_id"]

    # message schema as supplied in example data
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

    # parse kafka message schema
    parsed = df.select(
        from_json(col("value").cast("string"),
                  jsonschema).alias("parsed_value"))\
                          .select("parsed_value.*")

    # separate into regions (germany, global) and aggregate over the last minute
    edits = parsed\
            .withColumn("is_germany", col("server_name").rlike(r"de\..*"))\
            .groupBy("is_germany", window("timestamp", "1 minute"))\
            .count()\
            .withColumn("region_id",
                    when(col("is_germany") == True, lit(german_id))\
                    .when(col("is_germany") == False, lit(global_id)))\
            .withColumn("time", col("window.start"))\
            .select("time", "count", "region_id")

    print('output schema:')
    edits.printSchema()


    # define foreachBatch function to write to postgres database
    def foreach_batch_function(df, epoch_id):
        '''function requires epoch_id to work with foreachBatch method'''
        df.show()

        print("writing to db")
        df.write\
                .format("jdbc")\
                .option("url", "jdbc:postgresql://postgres:5432/wikipedia_events")\
                .option("truncate", "true")\
                .option("dbtable","public.events")\
                .option("user","user")\
                .option('driver', 'org.postgresql.Driver')\
                .option("password", "password")\
                .mode("overwrite")\
                .save()



    query_to_db = edits\
            .writeStream\
            .foreachBatch(foreach_batch_function)\
            .outputMode("complete")\
            .option("truncate", "true")\
            .option("checkpointLocation", "/tmp/checkpoints/wikipedia_streaming")\
            .start()

    query_to_db.awaitTermination()
    spark.stop()
