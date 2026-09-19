package com.amex.lumi.ingestion.api.dto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IngestionResponseTest {

    @Test
    void testIngestionResponse() {

        IngestionResponse response =
                new IngestionResponse(
                        "execution-123",
                        "Ingestion successful"
                );

        assertEquals(
                "execution-123",
                response.getExecutionId()
        );

        assertEquals(
                "Ingestion successful",
                response.getMessage()
        );
    }
}
