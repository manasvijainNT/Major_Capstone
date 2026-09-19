package com.amex.lumi.ingestion.api.controller;

import com.amex.lumi.ingestion.api.dto.IngestionRequest;
import com.amex.lumi.ingestion.api.dto.IngestionResponse;
import com.amex.lumi.ingestion.api.exception.ControlFileException;
import com.amex.lumi.ingestion.api.exception.InvalidInputFileException;
import com.amex.lumi.ingestion.api.service.IngestionService;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockMultipartFile;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class IngestionControllerTest {

    @Test
    void testStartIngestionSuccess() {

        IngestionService ingestionService =
                mock(IngestionService.class);

        IngestionController controller =
                new IngestionController(
                        ingestionService
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

        IngestionResponse serviceResponse =
                new IngestionResponse(
                        "execution-123",
                        "Small file ingestion DAG triggered successfully"
                );

        when(
                ingestionService.startIngestion(request)
        ).thenReturn(serviceResponse);

        var response =
                controller.startIngestion(request);

        assertNotNull(response);
        assertEquals(
                200,
                response.getStatusCode().value()
        );

        assertNotNull(response.getBody());

        assertEquals(
                "execution-123",
                response.getBody().getExecutionId()
        );

        assertEquals(
                "Small file ingestion DAG triggered successfully",
                response.getBody().getMessage()
        );
    }

    @Test
    void testFileIsRequired() {

        IngestionService ingestionService =
                mock(IngestionService.class);

        IngestionController controller =
                new IngestionController(
                        ingestionService
                );

        IngestionRequest request =
                new IngestionRequest();

        when(
                ingestionService.startIngestion(request)
        ).thenThrow(
                new InvalidInputFileException(
                        "File is required"
                )
        );

        InvalidInputFileException exception =
                assertThrows(
                        InvalidInputFileException.class,
                        () -> controller.startIngestion(request)
                );

        assertEquals(
                "File is required",
                exception.getMessage()
        );
    }

    @Test
    void testControlFileIsRequired() {

        IngestionService ingestionService =
                mock(IngestionService.class);

        IngestionController controller =
                new IngestionController(
                        ingestionService
                );

        IngestionRequest request =
                new IngestionRequest();

        when(
                ingestionService.startIngestion(request)
        ).thenThrow(
                new ControlFileException(
                        "Control file is required"
                )
        );

        ControlFileException exception =
                assertThrows(
                        ControlFileException.class,
                        () -> controller.startIngestion(request)
                );

        assertEquals(
                "Control file is required",
                exception.getMessage()
        );
    }
}