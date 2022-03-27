/opt/bitnami/spark/bin/spark-submit --master spark://spark:7077 \
--driver-memory 1G \
--executor-memory 1G \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,\
org.postgresql:postgresql:42.2.22 \
/opt/spark-jobs/$1
