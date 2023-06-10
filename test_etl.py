from context import get_local_spark_context

from datetime import datetime


def load_from_s3(context):
    timestamp = datetime.now()
    partition_str = f"year={timestamp.strftime('%Y')}/month={timestamp.strftime('%m')}/day={timestamp.strftime('%d')}"
    raw_folder_url = f"input/{partition_str}/7e0d748b-17f3-389b-a8aa-3990bf2bb4e2-2023-01-06T07_20_51.csv"
    path = f"s3a://raw/{raw_folder_url}"

    _frame = context.read.format("csv").option('header', 'True')

    return _frame.load(path)


def write_to_s3(frame=None):
    timestamp = datetime.now()
    partition_str = f"year={timestamp.strftime('%Y')}/month={timestamp.strftime('%m')}/day={timestamp.strftime('%d')}"
    bronze_folder_url = f"output/{partition_str}"
    path = f"s3a://bronze/{bronze_folder_url}"
    frame = frame.write.mode("append").format("parquet").option("compression","snappy")

    frame.save(path)


def test_etl(init_bucket):
    sc = get_local_spark_context()
    raw_dataframe = load_from_s3(sc)
    raw_dataframe.show(truncate=False)
    write_to_s3(raw_dataframe)
