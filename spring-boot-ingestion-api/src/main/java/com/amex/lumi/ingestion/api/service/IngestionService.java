package com.amex.lumi.ingestion.api.service;
import com.amex.lumi.ingestion.api.client.AirflowClient;
import com.amex.lumi.ingestion.api.dto.IngestionRequest;
import com.amex.lumi.ingestion.api.dto.IngestionResponse;
import com.amex.lumi.ingestion.api.exception.ControlFileException;
import com.amex.lumi.ingestion.api.exception.IngestionException;
import com.amex.lumi.ingestion.api.exception.InvalidInputFileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.unit.DataSize;
import org.springframework.web.multipart.MultipartFile;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

@Service
public class IngestionService {
    private static final Logger log =
            LoggerFactory.getLogger(IngestionService.class);

    private static final String AIRFLOW_INPUT_PATH =
            "/opt/airflow/input/";
    private static final String AIRFLOW_SPLIT_PATH =
            "/opt/airflow/input/phase2_split/";

    private final AirflowClient airflowClient;
    private final String airflowDagId;
    private final String uploadDirectory;
    private final DataSize largeFileThreshold;

    public IngestionService(
            AirflowClient airflowClient,
            @Value("${airflow.dag-id}") String airflowDagId,
            @Value("${ingestion.upload-directory}") String uploadDirectory,
            @Value("${ingestion.large-file-threshold}") DataSize largeFileThreshold
    ) {
        this.airflowClient = airflowClient;
        this.airflowDagId = airflowDagId;
        this.uploadDirectory = uploadDirectory;
        this.largeFileThreshold = largeFileThreshold;
    }
    public IngestionResponse startIngestion(
            IngestionRequest request
    ) {
        log.info("Ingestion request received");
        if (request == null) {
            throw new InvalidInputFileException(
                    "Ingestion request is required"
            );
        }
        MultipartFile file = request.getFileLocation();
        if (file == null || file.isEmpty()) {
            throw new InvalidInputFileException("File is required");
        }
        MultipartFile controlFile = request.getControlFile();
        if (controlFile == null || controlFile.isEmpty()) {
            throw new ControlFileException("Control file is required");
        }
        String originalFileName = file.getOriginalFilename();
        if (originalFileName == null || originalFileName.isBlank()) {
            throw new InvalidInputFileException("Invalid file name");
        }
        String fileName =
                Paths.get(originalFileName)
                        .getFileName()
                        .toString();
        String lowerCaseFileName =
                fileName.toLowerCase();
        if (!lowerCaseFileName.endsWith(".csv")
                && !lowerCaseFileName.endsWith(".json")) {
            throw new InvalidInputFileException(
                    "Only CSV and JSON files are supported"
            );
        }
        String controlFileOriginalName =
                controlFile.getOriginalFilename();
        if (controlFileOriginalName == null
                || controlFileOriginalName.isBlank()) {
            throw new ControlFileException(
                    "Invalid control file name"
            );
        }
        String controlFileName =
                Paths.get(controlFileOriginalName)
                        .getFileName()
                        .toString();
        if (!controlFileName
                .toLowerCase()
                .endsWith(".properties")) {
            throw new ControlFileException(
                    "Control file must be a .properties file"
            );
        }
        String executionId =
                UUID.randomUUID().toString();
        try {
            Path uploadPath =
                    Paths.get(uploadDirectory);
            Files.createDirectories(uploadPath);
            Path destination =
                    uploadPath.resolve(fileName);
            file.transferTo(destination);
            log.info(
                    "File uploaded successfully: {}",
                    fileName
            );
            Path controlFileDestination =
                    uploadPath.resolve(controlFileName);
            controlFile.transferTo(controlFileDestination);
            log.info(
                    "Control file uploaded successfully: {}",
                    controlFileName
            );

            String airflowControlFileLocation =
                    AIRFLOW_INPUT_PATH + controlFileName;

            long fileSize =
                    Files.size(destination);
            long thresholdSize =
                    largeFileThreshold.toBytes();

            log.info(
                    "Uploaded file size: {} bytes",
                    fileSize
            );
            log.info(
                    "Large file threshold: {} bytes",
                    thresholdSize
            );

            boolean isLargeFile =
                    fileSize > thresholdSize;
            if (!isLargeFile) {
                log.info(
                        "Small file detected. Sending directly to Airflow."
                );
                String airflowFileLocation =
                        AIRFLOW_INPUT_PATH
                                + fileName;
                airflowClient.triggerDag(
                        airflowDagId,
                        airflowFileLocation,
                        airflowControlFileLocation,
                        executionId
                );
                return new IngestionResponse(
                        executionId,
                        "Small file ingestion DAG triggered successfully"
                );
            }
            log.info(
                    "Large file detected. Starting PySpark file splitting."
            );

            String format =
                    lowerCaseFileName.endsWith(".csv")
                            ? "csv"
                            : "json";
            Path splitOutputDirectory =
                    uploadPath
                            .resolve("phase2_split")
                            .resolve(executionId);
            Files.createDirectories(
                    splitOutputDirectory
            );
            Path splitScript =
                    Paths.get(
                                    System.getProperty("user.dir")
                            )
                            .resolve("pyspark-splitter")
                            .resolve("split_file.py");
            log.info(
                    "Starting PySpark with script: {}",
                    splitScript
            );

            ProcessBuilder processBuilder =
                    new ProcessBuilder(
                            "C:\\Spark\\spark-3.5.6-bin-hadoop3-scala2.13\\Spark-3.5.6-bin-hadoop3-scala2.13\\bin\\spark-submit.cmd",
                            "--master",
                            "local[*]",
                            splitScript.toString(),
                            "--input",
                            destination.toString(),
                            "--output",
                            splitOutputDirectory.toString(),
                            "--format",
                            format
                    );

            processBuilder.environment().put(
                    "PYSPARK_PYTHON",
                    "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python310\\python.exe"
            );

            processBuilder.environment().put(
                    "PYSPARK_DRIVER_PYTHON",
                    "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python310\\python.exe"
            );

            processBuilder.environment().put(
                    "HADOOP_HOME",
                    "C:\\hadoop"
            );
            processBuilder.environment().put(
                    "hadoop.home.dir",
                    "C:\\hadoop"
            );
            processBuilder.environment().put(
                    "PATH",
                    "C:\\hadoop\\bin;"
                            + "C:\\Spark\\spark-3.5.6-bin-hadoop3-scala2.13\\Spark-3.5.6-bin-hadoop3-scala2.13\\bin;"
                            + processBuilder.environment().get("PATH")
            );
            processBuilder.directory(
                    Paths.get(
                            System.getProperty("user.dir")
                    ).toFile()
            );
            processBuilder.redirectErrorStream(true);
            Process process =
                    processBuilder.start();
            try (
                    BufferedReader reader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            process.getInputStream()
                                    )
                            )
            ) {
                String line;
                while (
                        (line = reader.readLine()) != null
                ) {
                    log.info(
                            "[PySpark] {}",
                            line
                    );
                }
            }
            int exitCode =
                    process.waitFor();
            if (exitCode != 0) {
                log.error(
                        "PySpark splitter failed. Exit code: {}",
                        exitCode
                );
                throw new IngestionException(
                        "PySpark file splitting failed"
                );
            }
            log.info(
                    "PySpark file splitting completed successfully."
            );
            List<Path> chunkFiles;
            try (
                    var files =
                            Files.list(
                                    splitOutputDirectory
                            )
            ) {
                chunkFiles =
                        files
                                .filter(Files::isRegularFile)
                                .filter(path ->
                                        path.getFileName()
                                                .toString()
                                                .startsWith("part-")
                                )
                                .filter(path ->
                                        path.getFileName()
                                                .toString()
                                                .endsWith(
                                                        "." + format
                                                )
                                )
                                .sorted(
                                        Comparator.comparing(
                                                path ->
                                                        path.getFileName()
                                                                .toString()
                                        )
                                )
                                .toList();
            }
            if (chunkFiles.isEmpty()) {
                throw new IngestionException(
                        "PySpark completed but no chunk files were generated"
                );
            }
            log.info(
                    "Number of chunk files generated: {}",
                    chunkFiles.size()
            );
            List<String> airflowChunkLocations =
                    new ArrayList<>();
            String airflowInputRoot =
                    AIRFLOW_SPLIT_PATH
                            + executionId;
            for (Path chunkFile : chunkFiles) {
                String chunkFileName =
                        chunkFile.getFileName()
                                .toString();
                airflowChunkLocations.add(
                        airflowInputRoot
                                + "/"
                                + chunkFileName
                );
            }
            log.info(
                    "Airflow chunk locations: {}",
                    airflowChunkLocations
            );
            airflowClient.triggerDagWithChunks(
                    airflowDagId,
                    airflowChunkLocations,
                    airflowControlFileLocation,
                    executionId
            );
            return new IngestionResponse(
                    executionId,
                    "Large file split successfully into "
                            + chunkFiles.size()
                            + " chunks. Airflow DAG triggered."
            );
        } catch (IOException e) {
            log.error(
                    "Failed during ingestion processing",
                    e
            );
            throw new IngestionException(
                    "Unable to process uploaded file",
                    e
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error(
                    "PySpark process was interrupted",
                    e
            );
            throw new IngestionException(
                    "PySpark process was interrupted",
                    e
            );
        }
    }
}