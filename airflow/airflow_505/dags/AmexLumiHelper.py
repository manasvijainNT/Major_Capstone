import os
import glob
import subprocess
import logging

import psycopg2

logger = logging.getLogger(__name__)

class AmexLumiHelper:

    @staticmethod
    def get_expected_count(control_file):

        with open(control_file, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line.startswith("record_count="):
                    return int(
                        line.split("=", 1)[1].strip()
                    )

        raise Exception(
            "record_count not found in control file"
        )

    @staticmethod
    def count_chunk_records(chunk):

        if not os.path.isfile(chunk):
            raise Exception(
                "Chunk file does not exist: " + chunk)

        record_count = 0

        with open(chunk, "r", encoding="utf-8") as file:
            for line in file:
                if line.strip():
                    record_count += 1

        # CSV header is not a data record
        if chunk.lower().endswith(".csv"):
            record_count -= 1
            if record_count < 0:
                record_count = 0
        return record_count


    @staticmethod
    def run_beam(
        file_location,
        execution_id,
        expected_count,
        error_file,
        jar_path
    ):

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

        logger.info(
            "Running Beam for file: %s",
            file_location
        )

        logger.info(
            "Execution ID: %s",
            execution_id
        )

        logger.info(
            "Expected Count: %s",
            expected_count
        )

        logger.info(
            "Error File: %s",
            error_file
        )

        result = subprocess.run(
            command,
            capture_output=True,
            text=True
        )

        # Beam standard output
        if result.stdout:

            logger.info(
                "Beam output:\n%s",
                result.stdout
            )

        # Beam error output
        if result.stderr:

            logger.error(
                "Beam error output:\n%s",
                result.stderr
            )

        # Check Beam exit code
        if result.returncode != 0:

            logger.error(
                "Beam ingestion failed for file: %s",
                file_location
            )

            logger.error(
                "Beam exit code: %s",
                result.returncode
            )

            raise Exception(
                "Beam ingestion failed for file: "
                + file_location
            )

        logger.info(
            "Beam processing completed successfully: %s",
            file_location
        )


    @staticmethod
    def get_actual_count(execution_id):

        query = """
            SELECT COUNT(*)
            FROM employee
            WHERE execution_id = %s
        """

        db_host = os.getenv(
            "LUMI_DB_HOST"
        )

        db_port = os.getenv(
            "LUMI_DB_PORT"
        )

        db_name = os.getenv(
            "LUMI_DB_NAME"
        )

        db_user = os.getenv(
            "LUMI_DB_USER"
        )

        db_password = os.getenv(
            "LUMI_DB_PASSWORD"
        )

        if not db_password:

            raise Exception(
                "LUMI_DB_PASSWORD environment variable is not set"
            )

        try:

            with psycopg2.connect(
                host=db_host,
                port=db_port,
                dbname=db_name,
                user=db_user,
                password=db_password
            ) as connection:

                with connection.cursor() as cursor:

                    cursor.execute(
                        query,
                        (execution_id,)
                    )

                    row = cursor.fetchone()

                    return int(row[0])

        except Exception as exc:

            raise Exception(
                "Unable to read employee count "
                "from PostgreSQL"
            ) from exc


    @staticmethod
    def get_error_files(execution_id):

        pattern = (
            f"/opt/airflow/errors/"
            f"error_records_{execution_id}*"
        )

        return glob.glob(pattern)


    @staticmethod
    def has_error_records(execution_id):

        error_files = (
            AmexLumiHelper.get_error_files(
                execution_id
            )
        )

        for error_file in error_files:

            if os.path.isfile(
                error_file
            ):

                file_size = os.path.getsize(
                    error_file
                )

                if file_size > 0:
                    return True

        return False

    @staticmethod
    def log_error_files(execution_id):

        error_files = (
            AmexLumiHelper.get_error_files(
                execution_id
            )
        )

        if not error_files:

            logger.info("No error files found.")

            return

        logger.info("Error files:")

        for error_file in error_files:

            if not os.path.isfile(
                error_file
            ):
                continue

            file_size = os.path.getsize(
                error_file
            )

            logger.info(
                "File: %s (%s bytes)",
                error_file,
                file_size
            )

        logger.info("Error Records:")

        for error_file in error_files:

            if not os.path.isfile(
                error_file
            ):
                continue

            logger.info(
                "Reading error file: %s",
                error_file
            )

            with open(error_file, "r", encoding="utf-8") as file:
                content = file.read()

            if content.strip():
                logger.info(
                    "Error file content:\n%s",
                    content
                )

            else:

                logger.info(
                    "Error file is empty."
                )