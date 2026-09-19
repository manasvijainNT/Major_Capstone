package com.amex.lumi.ingestion.api.dto;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockMultipartFile;

import static org.junit.jupiter.api.Assertions.*;

class IngestionRequestTest {

    @Test
    void testSetAndGetFileLocation() {

        IngestionRequest request =
                new IngestionRequest();

        MockMultipartFile file =
                new MockMultipartFile(
                        "fileLocation",
                        "employee.csv",
                        "text/csv",
                        "test data".getBytes()
                );

        request.setFileLocation(file);

        assertNotNull(
                request.getFileLocation()
        );

        assertEquals(
                "employee.csv",
                request.getFileLocation()
                        .getOriginalFilename()
        );
    }

    @Test
    void testSetAndGetControlFile() {

        IngestionRequest request =
                new IngestionRequest();

        MockMultipartFile controlFile =
                new MockMultipartFile(
                        "controlFile",
                        "control.properties",
                        "text/plain",
                        "record_count=1".getBytes()
                );

        request.setControlFile(controlFile);

        assertNotNull(
                request.getControlFile()
        );

        assertEquals(
                "control.properties",
                request.getControlFile()
                        .getOriginalFilename()
        );
    }
}