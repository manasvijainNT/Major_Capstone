package com.amex.lumi.ingestion.api.controller;

import com.amex.lumi.ingestion.api.exception.DecryptionException;
import com.amex.lumi.ingestion.api.exception.EmployeeNotFoundException;
import com.amex.lumi.ingestion.api.service.DecryptionService;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DecryptionControllerTest {

    @Test
    void testDecryptEmployeeSuccess() throws Exception {

        DecryptionService decryptionService =
                mock(DecryptionService.class);

        DecryptionController controller =
                new DecryptionController(
                        decryptionService
                );

        Map<String, Object> employee =
                Map.of(
                        "employee_id", "EMP001",
                        "first_name", "Manasvi",
                        "last_name", "Jain",
                        "phone_number", "9876543210",
                        "salary", "50000"
                );

        when(
                decryptionService
                        .getDecryptedEmployee("EMP001")
        ).thenReturn(employee);

        var response =
                controller.decryptEmployee("EMP001");

        assertNotNull(response);
        assertEquals(
                200,
                response.getStatusCode().value()
        );

        assertNotNull(response.getBody());

        assertEquals(
                "EMP001",
                response.getBody()
                        .get("employee_id")
        );

        assertEquals(
                "Manasvi",
                response.getBody()
                        .get("first_name")
        );

        assertEquals(
                "9876543210",
                response.getBody()
                        .get("phone_number")
        );
    }

    @Test
    void testEmployeeNotFound() throws Exception {

        DecryptionService decryptionService =
                mock(DecryptionService.class);

        DecryptionController controller =
                new DecryptionController(
                        decryptionService
                );

        when(
                decryptionService
                        .getDecryptedEmployee("EMP999")
        ).thenThrow(
                new EmployeeNotFoundException(
                        "Employee not found with ID: EMP999"
                )
        );

        EmployeeNotFoundException exception =
                assertThrows(
                        EmployeeNotFoundException.class,
                        () -> controller
                                .decryptEmployee("EMP999")
                );

        assertEquals(
                "Employee not found with ID: EMP999",
                exception.getMessage()
        );
    }

    @Test
    void testDecryptionError() throws Exception {

        DecryptionService decryptionService =
                mock(DecryptionService.class);

        DecryptionController controller =
                new DecryptionController(
                        decryptionService
                );

        when(
                decryptionService
                        .getDecryptedEmployee("EMP001")
        ).thenThrow(
                new DecryptionException(
                        "Unable to decrypt employee data"
                )
        );

        DecryptionException exception =
                assertThrows(
                        DecryptionException.class,
                        () -> controller
                                .decryptEmployee("EMP001")
                );

        assertEquals(
                "Unable to decrypt employee data",
                exception.getMessage()
        );
    }
}