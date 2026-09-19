package com.amex.lumi.ingestion.api.exception;

import com.amex.lumi.ingestion.api.dto.ErrorResponse;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler =
            new GlobalExceptionHandler();

    @Test
    void testHandleInvalidInputFile() {

        InvalidInputFileException exception =
                new InvalidInputFileException(
                        "File is required"
                );

        ResponseEntity<ErrorResponse> response =
                handler.handleInvalidInputFile(exception);

        assertEquals(
                HttpStatus.BAD_REQUEST,
                response.getStatusCode()
        );

        assertNotNull(response.getBody());

        assertEquals(
                400,
                response.getBody().getStatus()
        );

        assertEquals(
                "Bad Request",
                response.getBody().getError()
        );

        assertEquals(
                "File is required",
                response.getBody().getMessage()
        );
    }

    @Test
    void testHandleControlFile() {

        ControlFileException exception =
                new ControlFileException(
                        "Control file is required"
                );

        ResponseEntity<ErrorResponse> response =
                handler.handleControlFile(exception);

        assertEquals(
                HttpStatus.BAD_REQUEST,
                response.getStatusCode()
        );

        assertNotNull(response.getBody());

        assertEquals(
                400,
                response.getBody().getStatus()
        );

        assertEquals(
                "Bad Request",
                response.getBody().getError()
        );

        assertEquals(
                "Control file is required",
                response.getBody().getMessage()
        );
    }

    @Test
    void testHandleEmployeeNotFound() {

        EmployeeNotFoundException exception =
                new EmployeeNotFoundException(
                        "Employee not found with ID: EMP999"
                );

        ResponseEntity<ErrorResponse> response =
                handler.handleEmployeeNotFound(exception);

        assertEquals(
                HttpStatus.NOT_FOUND,
                response.getStatusCode()
        );

        assertNotNull(response.getBody());

        assertEquals(
                404,
                response.getBody().getStatus()
        );

        assertEquals(
                "Not Found",
                response.getBody().getError()
        );

        assertEquals(
                "Employee not found with ID: EMP999",
                response.getBody().getMessage()
        );
    }

    @Test
    void testHandleDecryption() {

        DecryptionException exception =
                new DecryptionException(
                        "Failed to decrypt employee data"
                );

        ResponseEntity<ErrorResponse> response =
                handler.handleDecryption(exception);

        assertEquals(
                HttpStatus.INTERNAL_SERVER_ERROR,
                response.getStatusCode()
        );

        assertNotNull(response.getBody());

        assertEquals(
                500,
                response.getBody().getStatus()
        );

        assertEquals(
                "Internal Server Error",
                response.getBody().getError()
        );

        assertEquals(
                "Failed to decrypt employee data",
                response.getBody().getMessage()
        );
    }

    @Test
    void testHandleIngestion() {

        IngestionException exception =
                new IngestionException(
                        "Unable to process uploaded file"
                );

        ResponseEntity<ErrorResponse> response =
                handler.handleIngestion(exception);

        assertEquals(
                HttpStatus.INTERNAL_SERVER_ERROR,
                response.getStatusCode()
        );

        assertNotNull(response.getBody());

        assertEquals(
                500,
                response.getBody().getStatus()
        );

        assertEquals(
                "Internal Server Error",
                response.getBody().getError()
        );

        assertEquals(
                "Unable to process uploaded file",
                response.getBody().getMessage()
        );
    }

    @Test
    void testHandleGenericException() {

        Exception exception =
                new Exception("Some unexpected error");

        ResponseEntity<ErrorResponse> response =
                handler.handleGenericException(exception);

        assertEquals(
                HttpStatus.INTERNAL_SERVER_ERROR,
                response.getStatusCode()
        );

        assertNotNull(response.getBody());

        assertEquals(
                500,
                response.getBody().getStatus()
        );

        assertEquals(
                "Internal Server Error",
                response.getBody().getError()
        );

        assertEquals(
                "An unexpected error occurred",
                response.getBody().getMessage()
        );
    }
}