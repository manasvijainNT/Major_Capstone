package com.amex.lumi.ingestion.api.service;

import com.amex.lumi.ingestion.api.client.AirflowClient;
import com.amex.lumi.ingestion.api.dto.IngestionRequest;
import com.amex.lumi.ingestion.api.dto.IngestionResponse;
import com.amex.lumi.ingestion.api.exception.ControlFileException;
import com.amex.lumi.ingestion.api.exception.IngestionException;
import com.amex.lumi.ingestion.api.exception.InvalidInputFileException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.util.unit.DataSize;

import org.mockito.MockedConstruction;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class IngestionServiceTest {

    @TempDir
    Path tempDirectory;

    @Test
    void testSmallFileIngestion() throws Exception {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "employee-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofMegabytes(10)
                );

        MockMultipartFile inputFile =
                new MockMultipartFile(
                        "fileLocation",
                        "employee.csv",
                        "text/csv",
                        "1,Manasvi,Jain\n".getBytes()
                );

        MockMultipartFile controlFile =
                new MockMultipartFile(
                        "controlFile",
                        "control.properties",
                        "text/plain",
                        "record_count=1".getBytes()
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(inputFile);
        request.setControlFile(controlFile);

        IngestionResponse response =
                service.startIngestion(request);

        assertNotNull(response);

        assertNotNull(
                response.getExecutionId()
        );

        assertEquals(
                "Small file ingestion DAG triggered successfully",
                response.getMessage()
        );

        verify(airflowClient).triggerDag(
                eq("employee-ingestion"),
                eq("/opt/airflow/input/employee.csv"),
                eq("/opt/airflow/input/control.properties"),
                anyString()
        );
    }

    @Test
    void testInvalidFileName() {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "amex-lumi-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofMegabytes(10)
                );

        MockMultipartFile file =
                new MockMultipartFile(
                        "fileLocation",
                        "",
                        "text/csv",
                        "test".getBytes()
                );

        MockMultipartFile controlFile =
                new MockMultipartFile(
                        "controlFile",
                        "control.properties",
                        "text/plain",
                        "record_count=1".getBytes()
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(file);
        request.setControlFile(controlFile);

        assertThrows(
                InvalidInputFileException.class,
                () -> service.startIngestion(request)
        );
    }

    @Test
    void testInvalidControlFileName() {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "amex-lumi-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofMegabytes(10)
                );

        MockMultipartFile file =
                new MockMultipartFile(
                        "fileLocation",
                        "employee.csv",
                        "text/csv",
                        "test".getBytes()
                );

        MockMultipartFile controlFile =
                new MockMultipartFile(
                        "controlFile",
                        "",
                        "text/plain",
                        "record_count=1".getBytes()
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(file);
        request.setControlFile(controlFile);

        assertThrows(
                ControlFileException.class,
                () -> service.startIngestion(request)
        );
    }

    @Test
    void testFileUploadFailure() throws Exception {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "amex-lumi-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofMegabytes(10)
                );

        MockMultipartFile file =
                mock(MockMultipartFile.class);

        when(file.isEmpty())
                .thenReturn(false);

        when(file.getOriginalFilename())
                .thenReturn("employee.csv");

        doThrow(new IOException("Upload failed"))
                .when(file)
                .transferTo(any(Path.class));

        MockMultipartFile controlFile =
                new MockMultipartFile(
                        "controlFile",
                        "control.properties",
                        "text/plain",
                        "record_count=1".getBytes()
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(file);
        request.setControlFile(controlFile);

        assertThrows(
                IngestionException.class,
                () -> service.startIngestion(request)
        );
    }

    @Test
    void testControlFileUploadFailure() throws Exception {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "amex-lumi-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofMegabytes(10)
                );

        MockMultipartFile file =
                new MockMultipartFile(
                        "fileLocation",
                        "employee.csv",
                        "text/csv",
                        "test".getBytes()
                );

        MockMultipartFile controlFile =
                mock(MockMultipartFile.class);

        when(controlFile.isEmpty())
                .thenReturn(false);

        when(controlFile.getOriginalFilename())
                .thenReturn("control.properties");

        doThrow(new IOException("Control file upload failed"))
                .when(controlFile)
                .transferTo(any(Path.class));

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(file);
        request.setControlFile(controlFile);

        assertThrows(
                IngestionException.class,
                () -> service.startIngestion(request)
        );
    }

    @Test
    void testRequestIsNull() {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "employee-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofMegabytes(10)
                );

        InvalidInputFileException exception =
                assertThrows(
                        InvalidInputFileException.class,
                        () -> service.startIngestion(null)
                );

        assertEquals(
                "Ingestion request is required",
                exception.getMessage()
        );
    }

    @Test
    void testFileIsRequired() {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "employee-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofMegabytes(10)
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(null);

        InvalidInputFileException exception =
                assertThrows(
                        InvalidInputFileException.class,
                        () -> service.startIngestion(request)
                );

        assertEquals(
                "File is required",
                exception.getMessage()
        );
    }

    @Test
    void testControlFileIsRequired() {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "employee-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofMegabytes(10)
                );

        MockMultipartFile inputFile =
                new MockMultipartFile(
                        "fileLocation",
                        "employee.csv",
                        "text/csv",
                        "1,Manasvi,Jain\n".getBytes()
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(inputFile);
        request.setControlFile(null);

        ControlFileException exception =
                assertThrows(
                        ControlFileException.class,
                        () -> service.startIngestion(request)
                );

        assertEquals(
                "Control file is required",
                exception.getMessage()
        );
    }

    @Test
    void testUnsupportedFileFormat() {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "employee-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofMegabytes(10)
                );

        MockMultipartFile inputFile =
                new MockMultipartFile(
                        "fileLocation",
                        "employee.txt",
                        "text/plain",
                        "some data".getBytes()
                );

        MockMultipartFile controlFile =
                new MockMultipartFile(
                        "controlFile",
                        "control.properties",
                        "text/plain",
                        "record_count=1".getBytes()
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(inputFile);
        request.setControlFile(controlFile);

        InvalidInputFileException exception =
                assertThrows(
                        InvalidInputFileException.class,
                        () -> service.startIngestion(request)
                );

        assertEquals(
                "Only CSV and JSON files are supported",
                exception.getMessage()
        );
    }

    @Test
    void testInvalidControlFileFormat() {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "employee-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofMegabytes(10)
                );

        MockMultipartFile inputFile =
                new MockMultipartFile(
                        "fileLocation",
                        "employee.csv",
                        "text/csv",
                        "1,Manasvi,Jain\n".getBytes()
                );

        MockMultipartFile controlFile =
                new MockMultipartFile(
                        "controlFile",
                        "control.txt",
                        "text/plain",
                        "record_count=1".getBytes()
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(inputFile);
        request.setControlFile(controlFile);

        ControlFileException exception =
                assertThrows(
                        ControlFileException.class,
                        () -> service.startIngestion(request)
                );

        assertEquals(
                "Control file must be a .properties file",
                exception.getMessage()
        );
    }

    @Test
    void testSmallJsonFileIngestion() throws Exception {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "employee-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofMegabytes(10)
                );

        MockMultipartFile inputFile =
                new MockMultipartFile(
                        "fileLocation",
                        "employee.json",
                        "application/json",
                        "{\"employee_id\":\"EMP001\"}".getBytes()
                );

        MockMultipartFile controlFile =
                new MockMultipartFile(
                        "controlFile",
                        "control.properties",
                        "text/plain",
                        "record_count=1".getBytes()
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(inputFile);
        request.setControlFile(controlFile);

        IngestionResponse response =
                service.startIngestion(request);

        assertNotNull(response);

        assertNotNull(
                response.getExecutionId()
        );

        assertEquals(
                "Small file ingestion DAG triggered successfully",
                response.getMessage()
        );

        verify(airflowClient).triggerDag(
                eq("employee-ingestion"),
                eq("/opt/airflow/input/employee.json"),
                eq("/opt/airflow/input/control.properties"),
                anyString()
        );
    }

    @Test
    void testEmptyInputFile() {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "employee-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofMegabytes(10)
                );

        MockMultipartFile inputFile =
                new MockMultipartFile(
                        "fileLocation",
                        "employee.csv",
                        "text/csv",
                        new byte[0]
                );

        MockMultipartFile controlFile =
                new MockMultipartFile(
                        "controlFile",
                        "control.properties",
                        "text/plain",
                        "record_count=0".getBytes()
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(inputFile);
        request.setControlFile(controlFile);

        InvalidInputFileException exception =
                assertThrows(
                        InvalidInputFileException.class,
                        () -> service.startIngestion(request)
                );

        assertEquals(
                "File is required",
                exception.getMessage()
        );
    }
    @Test
    void testEmptyControlFile() {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "employee-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofMegabytes(10)
                );

        MockMultipartFile inputFile =
                new MockMultipartFile(
                        "fileLocation",
                        "employee.csv",
                        "text/csv",
                        "1,Manasvi,Jain\n".getBytes()
                );

        MockMultipartFile controlFile =
                new MockMultipartFile(
                        "controlFile",
                        "control.properties",
                        "text/plain",
                        new byte[0]
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(inputFile);
        request.setControlFile(controlFile);

        ControlFileException exception =
                assertThrows(
                        ControlFileException.class,
                        () -> service.startIngestion(request)
                );

        assertEquals(
                "Control file is required",
                exception.getMessage()
        );
    }
    @Test
    void testLargeCsvFileIngestionSuccessfully() throws Exception {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "employee-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofBytes(1)
                );

        MockMultipartFile inputFile =
                new MockMultipartFile(
                        "fileLocation",
                        "large_employee.csv",
                        "text/csv",
                        "1,Manasvi,Jain\n2,Test,User\n"
                                .getBytes(StandardCharsets.UTF_8)
                );

        MockMultipartFile controlFile =
                new MockMultipartFile(
                        "controlFile",
                        "control.properties",
                        "text/plain",
                        "record_count=2"
                                .getBytes(StandardCharsets.UTF_8)
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(inputFile);
        request.setControlFile(controlFile);

        try (
                MockedConstruction<ProcessBuilder> mocked =
                        mockConstruction(
                                ProcessBuilder.class,
                                (builder, context) -> {

                                    when(builder.environment())
                                            .thenReturn(
                                                    new HashMap<>()
                                            );

                                    when(
                                            builder.redirectErrorStream(true)
                                    ).thenReturn(builder);

                                    when(
                                            builder.directory(any())
                                    ).thenReturn(builder);

                                    Process process =
                                            mock(Process.class);

                                    when(
                                            process.getInputStream()
                                    ).thenReturn(
                                            new ByteArrayInputStream(
                                                    "PySpark completed"
                                                            .getBytes(
                                                                    StandardCharsets.UTF_8
                                                            )
                                            )
                                    );

                                    when(
                                            process.waitFor()
                                    ).thenAnswer(invocation -> {

                                        Path splitRoot =
                                                tempDirectory.resolve(
                                                        "phase2_split"
                                                );

                                        Path executionDirectory =
                                                Files.list(splitRoot)
                                                        .findFirst()
                                                        .orElseThrow();

                                        Files.writeString(
                                                executionDirectory.resolve(
                                                        "part-00001.csv"
                                                ),
                                                "1,Manasvi,Jain\n"
                                        );

                                        Files.writeString(
                                                executionDirectory.resolve(
                                                        "part-00002.csv"
                                                ),
                                                "2,Test,User\n"
                                        );

                                        return 0;
                                    });

                                    when(builder.start())
                                            .thenReturn(process);
                                })
        ) {

            IngestionResponse response =
                    service.startIngestion(request);

            assertNotNull(response);

            assertNotNull(
                    response.getExecutionId()
            );

            assertEquals(
                    "Large file split successfully into 2 chunks. Airflow DAG triggered.",
                    response.getMessage()
            );

            verify(airflowClient)
                    .triggerDagWithChunks(
                            eq("employee-ingestion"),
                            eq(
                                    List.of(
                                            "/opt/airflow/input/phase2_split/"
                                                    + response.getExecutionId()
                                                    + "/part-00001.csv",
                                            "/opt/airflow/input/phase2_split/"
                                                    + response.getExecutionId()
                                                    + "/part-00002.csv"
                                    )
                            ),
                            eq(
                                    "/opt/airflow/input/control.properties"
                            ),
                            eq(response.getExecutionId())
                    );
        }
    }
    @Test
    void testLargeJsonFileIngestionSuccessfully()
            throws Exception {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "employee-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofBytes(1)
                );

        MockMultipartFile inputFile =
                new MockMultipartFile(
                        "fileLocation",
                        "large_employee.json",
                        "application/json",
                        "{\"employee_id\":\"EMP001\"}"
                                .getBytes(StandardCharsets.UTF_8)
                );

        MockMultipartFile controlFile =
                new MockMultipartFile(
                        "controlFile",
                        "control.properties",
                        "text/plain",
                        "record_count=1"
                                .getBytes(StandardCharsets.UTF_8)
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(inputFile);
        request.setControlFile(controlFile);

        try (
                MockedConstruction<ProcessBuilder> mocked =
                        mockConstruction(
                                ProcessBuilder.class,
                                (builder, context) -> {

                                    when(builder.environment())
                                            .thenReturn(
                                                    new HashMap<>()
                                            );

                                    when(
                                            builder.redirectErrorStream(true)
                                    ).thenReturn(builder);

                                    when(
                                            builder.directory(any())
                                    ).thenReturn(builder);

                                    Process process =
                                            mock(Process.class);

                                    when(
                                            process.getInputStream()
                                    ).thenReturn(
                                            new ByteArrayInputStream(
                                                    "PySpark completed"
                                                            .getBytes(
                                                                    StandardCharsets.UTF_8
                                                            )
                                            )
                                    );

                                    when(
                                            process.waitFor()
                                    ).thenAnswer(invocation -> {

                                        Path splitRoot =
                                                tempDirectory.resolve(
                                                        "phase2_split"
                                                );

                                        Path executionDirectory =
                                                Files.list(splitRoot)
                                                        .findFirst()
                                                        .orElseThrow();

                                        Files.writeString(
                                                executionDirectory.resolve(
                                                        "part-00001.json"
                                                ),
                                                "{\"employee_id\":\"EMP001\"}"
                                        );

                                        return 0;
                                    });

                                    when(builder.start())
                                            .thenReturn(process);
                                })
        ) {

            IngestionResponse response =
                    service.startIngestion(request);

            assertNotNull(response);

            assertEquals(
                    "Large file split successfully into 1 chunks. Airflow DAG triggered.",
                    response.getMessage()
            );

            verify(airflowClient)
                    .triggerDagWithChunks(
                            eq("employee-ingestion"),
                            eq(
                                    List.of(
                                            "/opt/airflow/input/phase2_split/"
                                                    + response.getExecutionId()
                                                    + "/part-00001.json"
                                    )
                            ),
                            eq(
                                    "/opt/airflow/input/control.properties"
                            ),
                            eq(response.getExecutionId())
                    );
        }
    }

    @Test
    void testPySparkProcessFailure() throws Exception {

        AirflowClient airflowClient =
                mock(AirflowClient.class);

        IngestionService service =
                new IngestionService(
                        airflowClient,
                        "employee-ingestion",
                        tempDirectory.toString(),
                        DataSize.ofBytes(1)
                );

        MockMultipartFile inputFile =
                new MockMultipartFile(
                        "fileLocation",
                        "large_employee.csv",
                        "text/csv",
                        "1,Manasvi,Jain\n"
                                .getBytes(StandardCharsets.UTF_8)
                );

        MockMultipartFile controlFile =
                new MockMultipartFile(
                        "controlFile",
                        "control.properties",
                        "text/plain",
                        "record_count=1"
                                .getBytes(StandardCharsets.UTF_8)
                );

        IngestionRequest request =
                new IngestionRequest();

        request.setFileLocation(inputFile);
        request.setControlFile(controlFile);

        try (
                MockedConstruction<ProcessBuilder> mocked =
                        mockConstruction(
                                ProcessBuilder.class,
                                (builder, context) -> {

                                    when(builder.environment())
                                            .thenReturn(
                                                    new HashMap<>()
                                            );

                                    when(
                                            builder.redirectErrorStream(true)
                                    ).thenReturn(builder);

                                    when(
                                            builder.directory(any())
                                    ).thenReturn(builder);

                                    Process process =
                                            mock(Process.class);

                                    when(
                                            process.getInputStream()
                                    ).thenReturn(
                                            new ByteArrayInputStream(
                                                    "PySpark failed"
                                                            .getBytes(
                                                                    StandardCharsets.UTF_8
                                                            )
                                            )
                                    );

                                    when(
                                            process.waitFor()
                                    ).thenReturn(1);

                                    when(builder.start())
                                            .thenReturn(process);
                                })
        ) {

            IngestionException exception =
                    assertThrows(
                            IngestionException.class,
                            () -> service.startIngestion(request)
                    );

            assertEquals(
                    "PySpark file splitting failed",
                    exception.getMessage()
            );

            verify(
                    airflowClient,
                    never()
            ).triggerDagWithChunks(
                    any(),
                    any(),
                    any(),
                    any()
            );
        }
    }
}