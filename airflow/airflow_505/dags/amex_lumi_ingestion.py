from datetime import datetime
import os
import logging

from airflow import DAG
from AmexLumiHelper import AmexLumiHelper
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator
)
from airflow.utils.trigger_rule import TriggerRule
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


MAX_PARALLEL_CHUNKS = 2

BEAM_JAR_PATH = (
    "/opt/airflow/beam/"
    "beam-ingestion-0.0.1-SNAPSHOT.jar"
)

ERROR_DIRECTORY = "/opt/airflow/errors"

def start_ingestion(**context):

    execution_id = context["dag_run"].conf.get("execution_id")

    if not execution_id:
        raise ValueError(
            "execution_id was not provided in DAG configuration"
        )

    logger.info("Starting Amex Lumi Ingestion")
    logger.info("Execution ID: %s", execution_id)


def validate_input(**context):

    conf = context["dag_run"].conf

    file_location = conf.get("file_location")
    chunk_locations = conf.get("chunk_locations")

    logger.info("Validating Input")

    # SMALL FILE
    if file_location:

        logger.info("Single file detected")
        logger.info("File: %s", file_location)

        if not os.path.isfile(file_location):
            raise Exception(
                "Input file does not exist: "
                + file_location
            )

    # LARGE FILE
    elif chunk_locations:

        logger.info("Split file detected")
        logger.info("Number of chunks: %s", len(chunk_locations))


        for index, chunk in enumerate(
            chunk_locations,
            start=1
        ):

            logger.info(f"Checking chunk %s: %s", index, chunk)

            if not os.path.isfile(chunk):
                raise Exception(
                    "Chunk file does not exist: " + chunk)

    # NO INPUT
    else:

        raise Exception(
            "Neither file_location nor "
            "chunk_locations was provided"
        )

    logger.info("Input validation successful")


def check_processing_type(**context):

    conf = context["dag_run"].conf

    file_location = conf.get("file_location")
    chunk_locations = conf.get("chunk_locations")

    logger.info("Checking process type")

    # LARGE FILE
    if chunk_locations:

        logger.info("Processing type: LARGE FILE")
        logger.info("Number of chunks: %s", len(chunk_locations))

        return "process_chunks"

    # SMALL FILE
    if file_location:

        logger.info("Processing type: SMALL FILE")
        logger.info("File: %s",file_location)

        return "run_beam_ingestion"

    raise Exception(
        "No input file or chunks provided"
    )


def process_chunks(**context):

    conf = context["dag_run"].conf
    chunks = conf.get("chunk_locations")

    if not chunks:
        raise Exception("No chunk locations were provided")

    logger.info("Processing Split Chunks")
    logger.info("Total chunks: %s", len(chunks))

    for index, chunk in enumerate(chunks, start=1):
        logger.info(f"Chunk %s: %s", index, chunk)


def run_beam_for_chunk(
    chunk,
    index,
    execution_id
):

    logger.info("Starting Beam for chunk: %s", index)
    logger.info("Chunk location: %s", chunk)

    chunk_record_count = (
        AmexLumiHelper.count_chunk_records(chunk)
    )

    logger.info(
        "Chunk %s expected records: %s",
        index,
        chunk_record_count
    )

    error_file = (
        ERROR_DIRECTORY
        + "/error_records_"
        + execution_id
        + "_"
        + str(index)
    )

    logger.info(
        "Error file: %s",
        error_file
    )

    AmexLumiHelper.run_beam(
        file_location=chunk,
        execution_id=execution_id,
        expected_count=chunk_record_count,
        error_file=error_file,
        jar_path=BEAM_JAR_PATH
    )

    logger.info(
        "Chunk %s processed successfully",
        index
    )

    return index

def run_beam_ingestion(**context):

    conf = context["dag_run"].conf

    execution_id = conf["execution_id"]
    control_file = conf["control_file_location"]
    file_location = conf.get("file_location", "")
    chunk_locations = conf.get("chunk_locations", [])

    logger.info("Starting Beam Ingestion")
    logger.info("Execution ID: %s", execution_id)
    logger.info("Control File: %s", control_file)

    # READ EXPECTED RECORD COUNT
    expected_count = (
        AmexLumiHelper.get_expected_count(control_file)
    )

    logger.info(
        "Expected Record Count: %s",
        expected_count
    )

    # CREATE ERROR DIRECTORY
    os.makedirs(
        ERROR_DIRECTORY,
        exist_ok=True
    )

    # SMALL FILE
    if file_location:
        logger.info("SMALL FILE PROCESSING")
        logger.info("File: %s", file_location)

        error_file = (
            ERROR_DIRECTORY
            + "/error_records_"
            + execution_id
        )

        AmexLumiHelper.run_beam(
             file_location=file_location,
             execution_id=execution_id,
             expected_count=expected_count,
             error_file=error_file,
             jar_path=BEAM_JAR_PATH
        )

        logger.info("Small file processed successfully")

    # LARGE FILE
    else:

        logger.info("LARGE FILE PROCESSING")
        logger.info("Chunks: %s", chunk_locations)

        if not chunk_locations:
            raise Exception(
                "No chunks found for large file processing"
            )

        logger.info(
            "Total chunks: %s",
            len(chunk_locations)
        )

        logger.info(
            "Maximum parallel chunks: %s",
            MAX_PARALLEL_CHUNKS
        )

        futures = []


        with ThreadPoolExecutor(
            max_workers=MAX_PARALLEL_CHUNKS
        ) as executor:

            for index, chunk in enumerate(
                chunk_locations,
                start=1
            ):

                future = executor.submit(
                    run_beam_for_chunk,
                    chunk,
                    index,
                    execution_id,
                )

                futures.append(future)

            # WAIT FOR ALL CHUNKS
            for future in as_completed(futures):
                completed_chunk = future.result()

                logger.info(
                    "Completed chunk: %s",
                    completed_chunk
                )

        logger.info("All Beam chunks completed successfully")

    logger.info("Beam Ingestion Completed")


def check_error_records(**context):

    execution_id = context["dag_run"].conf["execution_id"]

    logger.info("Checking Error Records")
    logger.info("Execution ID: %s", execution_id)

    if AmexLumiHelper.has_error_records(execution_id):

        logger.warning("ERROR RECORDS FOUND")

        return "report_validation_errors"

    logger.info("NO ERROR RECORDS FOUND")

    return "all_records_valid"


def all_records_valid(**context):

    execution_id = context["dag_run"].conf["execution_id"]
    logger.info("All records are valid")
    logger.info("Execution ID: %s",execution_id)
    logger.info("No validation errors found")


def report_validation_errors(**context):

    execution_id = context["dag_run"].conf["execution_id"]

    logger.info("ERROR RECORDS FOUND")
    logger.info("Execution ID: %s", execution_id)

    AmexLumiHelper.log_error_files(execution_id)

    logger.info("Valid records were processed.")
    logger.info(
        "Invalid records were written to error files."
    )


def validate_record_count(**context):

    conf = context["dag_run"].conf
    execution_id = conf["execution_id"]
    control_file = conf["control_file_location"]

    logger.info("FINAL RECORD COUNT VALIDATION")

    # EXPECTED COUNT
    expected_count = (
        AmexLumiHelper.get_expected_count(control_file)
    )

    logger.info("Expected Count: %s", expected_count)

    # ACTUAL POSTGRESQL COUNT
    actual_count= (
         AmexLumiHelper.get_actual_count(execution_id)
    )

    logger.info("PostgreSQL Count: %s", actual_count)

    # COMPARE COUNTS
    if actual_count != expected_count:

        logger.error("RECORD COUNT VALIDATION FAILED")

        raise Exception(
            "Record count mismatch! "
            f"Expected: {expected_count}, "
            f"PostgreSQL Actual: {actual_count}"
        )

    logger.info("RECORD COUNT VALIDATION SUCCESSFUL")


def ingestion_completed(**context):

    execution_id = context["dag_run"].conf["execution_id"]

    logger.info("Amex Lumi Ingestion Completed")

    logger.info("Execution ID: %s", execution_id)
    logger.info("All ingestion validations completed successfully.")


# DAG DEFINITION
with DAG(
    dag_id="amex_lumi_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "amex-lumi",
        "phase1",
        "phase2",
        "phase3"
    ]
) as dag:


    start_ingestion_task = PythonOperator(
        task_id="start_ingestion",
        python_callable=start_ingestion
    )

    validate_input_file_task = PythonOperator(
        task_id="validate_input_file",
        python_callable=validate_input
    )

    check_processing_type_task = BranchPythonOperator(
        task_id="check_processing_type",
        python_callable=check_processing_type
    )

    process_chunks_task = PythonOperator(
        task_id="process_chunks",
        python_callable=process_chunks
    )

    run_beam_ingestion_task = PythonOperator(
        task_id="run_beam_ingestion",
        python_callable=run_beam_ingestion,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    check_error_records_task = BranchPythonOperator(
        task_id="check_error_records",
        python_callable=check_error_records
    )

    report_validation_errors_task = PythonOperator(
        task_id="report_validation_errors",
        python_callable=report_validation_errors
    )

    all_records_valid_task = PythonOperator(
        task_id="all_records_valid",
        python_callable=all_records_valid
    )

    validate_record_count_task = PythonOperator(
        task_id="validate_record_count",
        python_callable=validate_record_count,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    ingestion_completed_task = PythonOperator(
        task_id="ingestion_completed",
        python_callable=ingestion_completed
    )

    # DAG DEPENDENCIES
    start_ingestion_task >> validate_input_file_task

    validate_input_file_task >> check_processing_type_task

    check_processing_type_task >> process_chunks_task
    check_processing_type_task >> run_beam_ingestion_task

    process_chunks_task >> run_beam_ingestion_task

    run_beam_ingestion_task >> check_error_records_task

    check_error_records_task >> report_validation_errors_task
    check_error_records_task >> all_records_valid_task

    report_validation_errors_task >> validate_record_count_task
    all_records_valid_task >> validate_record_count_task

    validate_record_count_task >> ingestion_completed_task
