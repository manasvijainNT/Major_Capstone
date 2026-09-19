from datetime import datetime
import os
import subprocess
import glob
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

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

    if file_location:

        logger.info("Single file detected")
        logger.info("File: %s", file_location)

        if not os.path.isfile(file_location):
            raise Exception(
                "Input file does not exist: "
                + file_location
            )

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

    if chunk_locations:

        logger.info("Processing type: LARGE FILE")
        logger.info("Number of chunks: %s", len(chunk_locations))

        return "process_chunks"

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
        raise Exception(
            "No chunk locations were provided"
        )

    logger.info("Processing Split Chunks")
    logger.info("Total chunks: %s", len(chunks))

    for index, chunk in enumerate(chunks, start=1):

        logger.info(f"Chunk %s: %s", index, chunk)

        if not os.path.isfile(chunk):
            raise Exception(
                "Chunk file does not exist: "
                + chunk
            )

    logger.info("All chunks are available")


def run_beam_for_chunk(
    chunk,
    index,
    execution_id,
    jar_path,
    error_directory
):

    logger.info("Starting Beam for chunk: %s", index)
    logger.info("Chunk location: %s", chunk)

    if not os.path.isfile(chunk):

        raise Exception(
            "Chunk file does not exist: "
            + chunk
        )

    chunk_record_count = 0

    with open(
        chunk,
        "r",
        encoding="utf-8"
    ) as file:

        for line in file:

            line = line.strip()

            if line:
                chunk_record_count += 1

    # CSV header is not a record
    if chunk.lower().endswith(".csv"):

        chunk_record_count -= 1

        if chunk_record_count < 0:
            chunk_record_count = 0

    logger.info(
        "Chunk %s expected records: %s",
        index,
        chunk_record_count
    )


    error_file = (
        error_directory
        + "/error_records_"
        + execution_id
        + "_"
        + str(index)
    )

    logger.info(
        "Error file: %s",
        error_file
    )


    command = [

        "java",

        "-jar",
        jar_path,

        "--file_location",
        chunk,

        "--execution_id",
        execution_id,

        "--expected_count",
        str(chunk_record_count),

        "--error_file",
        error_file
    ]

    logger.info("Running Beam command for chunk: %s", index)

    result = subprocess.run(
        command,
        capture_output=True,
        text=True
    )
    logger.info(
        "Beam output for chunk %s:",
        index
    )

    logger.info(result.stdout)

    if result.stderr:
        logger.info(
             "Beam error output for chunk %s:",
             index
        )
        logger.info(result.stderr)

    if result.returncode != 0:

        logger.info(
            "Beam failed for chunk: %s",
            index
        )

        logger.info(
            "Exit code: %s",
            result.returncode
        )

        raise Exception(
            "Beam ingestion failed for chunk "
            + str(index)
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

    jar_path = (
        "/opt/airflow/beam/"
        "beam-ingestion-0.0.1-SNAPSHOT.jar"
    )

    error_directory = "/opt/airflow/errors"
    logger.info("Starting Beam Ingestion")

    logger.info("Execution ID: %s", execution_id)
    logger.info("Control File: %s", control_file)

    expected_count = None

    with open(
        control_file,
        "r",
        encoding="utf-8"
    ) as file:

        for line in file:

            line = line.strip()

            if line.startswith("record_count="):

                expected_count = int(
                    line.split("=", 1)[1].strip()
                )

                break

    if expected_count is None:

        raise Exception(
            "record_count not found in control file"
        )

    logger.info(
        "Expected Record Count: %s",
        expected_count
    )

    os.makedirs(
        error_directory,
        exist_ok=True
    )


    if file_location:
        logger.info("SMALL FILE PROCESSING")
        logger.info("File: %s", file_location)

        error_file = (
            error_directory
            + "/error_records_"
            + execution_id
        )

        command = [

            "java",

            "-jar",

            jar_path,

            "--file_location",
            file_location,

            "--execution_id",
            execution_id,

            "--expected_count",
            str(expected_count),

            "--error_file",
            error_file
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True
        )

        logger.info(result.stdout)

        if result.stderr:
            logger.info(result.stderr)

        if result.returncode != 0:

            logger.info("Beam ingestion failed")

            raise Exception(
                f"Beam ingestion failed for file: "
                f"{file_location}"
            )

        logger.info("Small file processed successfully")


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

        MAX_PARALLEL_CHUNKS = 2

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
                    jar_path,
                    error_directory
                )

                futures.append(future)


            for future in as_completed(futures):

                completed_chunk = future.result()

                logger.info(
                    "Completed chunk: %s",
                    completed_chunk
                )


        logger.info("All Beam chunks completed successfully")

    logger.info("Beam Ingestion Completed")

def check_error_records(**context):

    execution_id = context["dag_run"].conf[
        "execution_id"
    ]

    pattern = (
        f"/opt/airflow/errors/"
        f"error_records_{execution_id}*"
    )

    error_files = glob.glob(pattern)


    logger.info("Checking Error Records")

    logger.info(
        "Execution ID: %s",
        execution_id
    )

    errors_found = False

    for error_file in error_files:

        if os.path.isfile(error_file):

            size = os.path.getsize(
                error_file
            )

            logger.info("Error file: %s", error_file)

            logger.info("Size: %s bytes", size)

            if size > 0:
                errors_found = True

    if errors_found:

        logger.warning("ERROR RECORDS FOUND")

        return "report_validation_errors"

    logger.info("NO ERROR RECORDS FOUND")

    return "all_records_valid"


def all_records_valid(**context):

    execution_id = context["dag_run"].conf["execution_id"]
    logger.info("All records are valid")
    logger.info("Execution ID: %s",execution_id)
    logger.info("No validation errors found")



def validate_record_count(**context):

    conf = context["dag_run"].conf

    execution_id = conf["execution_id"]
    control_file = conf["control_file_location"]

    logger.info("FINAL RECORD COUNT VALIDATION")


    expected_count = None

    with open(control_file, "r") as file:

        for line in file:

            line = line.strip()

            if line.startswith("record_count="):

                expected_count = int(
                    line.split("=", 1)[1].strip()
                )

                break

    if expected_count is None:

        raise Exception(
            "record_count not found in control file"
        )

    logger.info("Expected Count: %s", expected_count)



    import os
    import psycopg2

    db_host = os.getenv("LUMI_DB_HOST")
    db_port = os.getenv("LUMI_DB_PORT")
    db_name = os.getenv("LUMI_DB_NAME")
    db_user = os.getenv("LUMI_DB_USER")
    db_password = os.getenv("LUMI_DB_PASSWORD")

    if not db_password:

        raise Exception(
            "LUMI_DB_PASSWORD environment variable is not set"
        )

    query = """
        SELECT COUNT(*)
        FROM employee
        WHERE execution_id = %s
    """

    connection = None
    cursor = None

    try:

        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )

        cursor = connection.cursor()

        cursor.execute(
            query,
            (execution_id,)
        )

        row = cursor.fetchone()

        actual_count = int(row[0])

    except Exception as exc:

        raise Exception(
            "Unable to read employee count "
            "from PostgreSQL"
        ) from exc

    finally:

        if cursor:
            cursor.close()

        if connection:
            connection.close()

    logger.info("PostgreSQL Count: %s", actual_count)

    if actual_count != expected_count:

        logger.info("RECORD COUNT VALIDATION FAILED")


        raise Exception(
            "Record count mismatch! "
            f"Expected: {expected_count}, "
            f"PostgreSQL Actual: {actual_count}"
        )


    logger.info("RECORD COUNT VALIDATION SUCCESSFUL")

def report_validation_errors(**context):

    execution_id = context["dag_run"].conf["execution_id"]

    error_pattern = (
        f"/opt/airflow/errors/"
        f"error_records_{execution_id}*"
    )


    logger.info("ERROR RECORDS FOUND")

    logger.info("Execution ID: %s", execution_id)

    error_files = glob.glob(error_pattern)

    if not error_files:

        logger.info("No error files found.")

        return

    logger.info("Error files:")

    for error_file in error_files:

        if os.path.isfile(error_file):

            file_size = os.path.getsize(error_file)

            logger.info(
                "File: %s (%s bytes)",
                error_file,
                file_size
            )

    logger.info("Error Records:")

    for error_file in error_files:

        if not os.path.isfile(error_file):
            continue

        logger.info("File: %s", error_file)

        with open(
            error_file,
            "r",
            encoding="utf-8"
        ) as file:

            content = file.read()

        if content.strip():

            logger.info("Error file content: \n%s", content)

        else:

            logger.info("Error file is empty.")

    logger.info(
        "Valid records were processed."
    )

    logger.info(
        "Invalid records were written "
        "to error files."
    )


def ingestion_completed(**context):

    execution_id = context["dag_run"].conf["execution_id"]

    logger.info("Amex Lumi Ingestion Completed")

    logger.info("Execution ID: %s", execution_id)
    logger.info("All ingestion validations completed successfully.")



with DAG(
    dag_id="amex_lumi_ingestion",

    start_date=datetime(
        2026,
        1,
        1
    ),

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
        python_callable=ingestion_completed,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )


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
