package com.amex.lumi.ingestion.api.service;

import com.amex.lumi.ingestion.api.exception.DecryptionException;
import com.amex.lumi.ingestion.api.exception.EmployeeNotFoundException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DecryptionServiceTest {

    private static final String SECRET_KEY =
            "AmexLumiSecretK1";

    @Test
    void testDecryptEmployee() throws Exception {

        EmployeeFetchService employeeFetchService =
                mock(EmployeeFetchService.class);

        DecryptionService service =
                new DecryptionService(
                        employeeFetchService,
                        SECRET_KEY
                );

        String encryptedPhone =
                encrypt("9876543210");

        String encryptedSalary =
                encrypt("50000");

        Map<String, Object> employee =
                new HashMap<>();

        employee.put(
                "employee_id",
                "EMP001"
        );

        employee.put(
                "first_name",
                "Manasvi"
        );

        employee.put(
                "phone_number",
                encryptedPhone
        );

        employee.put(
                "salary",
                encryptedSalary
        );

        when(
                employeeFetchService
                        .getEmployee("EMP001")
        ).thenReturn(employee);

        Map<String, Object> result =
                service.getDecryptedEmployee("EMP001");

        assertNotNull(result);

        assertEquals(
                "EMP001",
                result.get("employee_id")
        );

        assertEquals(
                "Manasvi",
                result.get("first_name")
        );

        assertEquals(
                "9876543210",
                result.get("phone_number")
        );

        assertEquals(
                "50000",
                result.get("salary")
        );
    }

    @Test
    void testEmployeeNotFound() {

        EmployeeFetchService employeeFetchService =
                mock(EmployeeFetchService.class);

        DecryptionService service =
                new DecryptionService(
                        employeeFetchService,
                        SECRET_KEY
                );

        when(
                employeeFetchService
                        .getEmployee("EMP999")
        ).thenThrow(
                new EmployeeNotFoundException(
                        "Employee not found with ID: EMP999"
                )
        );

        EmployeeNotFoundException exception =
                assertThrows(
                        EmployeeNotFoundException.class,
                        () -> service
                                .getDecryptedEmployee("EMP999")
                );

        assertEquals(
                "Employee not found with ID: EMP999",
                exception.getMessage()
        );
    }

    @Test
    void testInvalidEncryptedData() {

        EmployeeFetchService employeeFetchService =
                mock(EmployeeFetchService.class);

        DecryptionService service =
                new DecryptionService(
                        employeeFetchService,
                        SECRET_KEY
                );

        Map<String, Object> employee =
                new HashMap<>();

        employee.put(
                "employee_id",
                "EMP001"
        );

        employee.put(
                "phone_number",
                "invalid-encrypted-data"
        );

        when(
                employeeFetchService
                        .getEmployee("EMP001")
        ).thenReturn(employee);

        assertThrows(
                DecryptionException.class,
                () -> service
                        .getDecryptedEmployee("EMP001")
        );
    }

    private String encrypt(String plainText)
            throws Exception {

        MessageDigest digest =
                MessageDigest.getInstance("SHA-256");

        byte[] keyBytes =
                digest.digest(
                        SECRET_KEY.getBytes(
                                StandardCharsets.UTF_8
                        )
                );

        SecretKeySpec secretKey =
                new SecretKeySpec(
                        keyBytes,
                        "AES"
                );

        Cipher cipher =
                Cipher.getInstance(
                        "AES/GCM/NoPadding"
                );

        byte[] iv = new byte[12];

        new java.security.SecureRandom()
                .nextBytes(iv);

        GCMParameterSpec parameterSpec =
                new GCMParameterSpec(
                        128,
                        iv
                );

        cipher.init(
                Cipher.ENCRYPT_MODE,
                secretKey,
                parameterSpec
        );

        byte[] encrypted =
                cipher.doFinal(
                        plainText.getBytes(
                                StandardCharsets.UTF_8
                        )
                );

        return Base64.getEncoder()
                .encodeToString(iv)
                + ":"
                + Base64.getEncoder()
                .encodeToString(encrypted);
    }
}