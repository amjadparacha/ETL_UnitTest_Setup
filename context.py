from pyspark.sql import SparkSession
from pyspark import SparkConf


def get_local_spark_context():
    conf = SparkConf() \
        .setMaster("local[*]") \
        .setAppName("test") \
        .set("spark.driver.memory", "5g") \
        .set("spark.driver.host", "127.0.0.1") \
        .set("spark.driver.port", "8082") \
        .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2")

    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .set("spark.hadoop.fs.s3a.path.style.access", "true") \
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .set("spark.hadoop.fs.s3a.aws.credentials.provider",
             "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .set("spark.hadoop.fs.s3a.access.key", "mock") \
        .set("spark.hadoop.fs.s3a.secret.key", "mock") \
        .set("spark.hadoop.fs.s3a.session.token", "mock") \
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")

    spark_context = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

    return spark_context
