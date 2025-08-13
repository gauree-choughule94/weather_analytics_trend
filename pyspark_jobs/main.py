from spark_utils import get_spark_session
from batch_processor import process_batches

if __name__ == "__main__":
    spark = get_spark_session()
    try:
        process_batches(spark)
    finally:
        spark.stop()
