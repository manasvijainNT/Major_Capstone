import argparse
import logging
import os
import math

from pyspark.sql import SparkSession


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

TARGET_PARTITION_SIZE_MB = 1.5

def calculate_partitions(input_file):

    file_size_bytes = os.path.getsize(input_file)
    file_size_mb = file_size_bytes / (1024 * 1024)

    target_partition_bytes = (
        TARGET_PARTITION_SIZE_MB * 1024 * 1024
    )

    partitions = max(
        1,
        math.ceil(file_size_bytes / target_partition_bytes)
    )

    logger.info("Input file size: %.2f MB", file_size_mb)
    logger.info(
        "Target partition size: %.2f MB",
        TARGET_PARTITION_SIZE_MB
    )
    logger.info(
        "Dynamically calculated partitions: %s",
        partitions
    )

    return partitions


def split_csv(spark, input_file, output_directory):

    logger.info("Starting CSV file split")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(input_file)
    )

    record_count = df.count()

    logger.info("Input file: %s", input_file)
    logger.info("Total records: %s", record_count)

    number_of_partitions = calculate_partitions(input_file)

    logger.info(
        "Repartitioning CSV into %s partitions",
        number_of_partitions
    )

    df = df.repartition(number_of_partitions)

    logger.info(
        "Actual Spark partitions after repartition: %s",
        df.rdd.getNumPartitions()
    )

    (
        df
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_directory)
    )

    logger.info("CSV file split completed successfully")
    logger.info("Output directory: %s", output_directory)


def split_json(spark, input_file, output_directory):

    logger.info("Starting JSON file split")

    df = (
         spark.read
         .option("multiline", True)
         .json(input_file)
    )

    record_count = df.count()

    logger.info("Input file: %s", input_file)
    logger.info("Total records: %s", record_count)

    number_of_partitions = calculate_partitions(input_file)

    logger.info(
        "Repartitioning JSON into %s partitions",
        number_of_partitions
    )

    df = df.repartition(number_of_partitions)

    logger.info(
        "Actual Spark partitions after repartition: %s",
        df.rdd.getNumPartitions()
    )

    (
        df
        .write
        .mode("overwrite")
        .json(output_directory)
    )

    logger.info("JSON file split completed successfully")
    logger.info("Output directory: %s", output_directory)


def main():

    parser = argparse.ArgumentParser(
        description="Amex Lumi PySpark File Splitter"
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Input file location"
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output directory"
    )

    parser.add_argument(
        "--format",
        required=True,
        choices=["csv", "json"],
        help="Input file format"
    )

    args = parser.parse_args()

    if not os.path.isfile(args.input):
        raise FileNotFoundError(
            f"Input file does not exist: {args.input}"
        )

    spark = (
        SparkSession.builder
        .appName("AmexLumiFileSplitter")
        .master("local[*]")
        .getOrCreate()
    )

    try:

        if args.format == "csv":

            split_csv(
                spark,
                args.input,
                args.output
            )

        elif args.format == "json":

            split_json(
                spark,
                args.input,
                args.output
            )

    finally:

        spark.stop()

if __name__ == "__main__":
    main()