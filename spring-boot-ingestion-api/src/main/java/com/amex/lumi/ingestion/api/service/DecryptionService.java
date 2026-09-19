package com.amex.lumi.ingestion.api.service;

import com.amex.lumi.ingestion.api.exception.DecryptionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Map;

@Service
public class DecryptionService {

    private final EmployeeFetchService employeeFetchService;
    private String secretKey;

    public DecryptionService(
            EmployeeFetchService employeeFetchService,
            @Value("${lumi.encryption.secret-key}")String secretKey) {

        this.employeeFetchService = employeeFetchService;
        this.secretKey = secretKey;
    }

    public Map<String, Object> getDecryptedEmployee(
            String employeeId) {

        Map<String, Object> employee =
                employeeFetchService.getEmployee(employeeId);

        if (employee == null || employee.isEmpty()) {
            return employee;
        }

        // Decrypt phone number
        if (employee.get("phone_number") != null) {
            employee.put(
                    "phone_number",
                    decrypt(
                            employee.get("phone_number")
                                    .toString()
                    )
            );
        }

        // Decrypt salary
        if (employee.get("salary") != null) {
            employee.put(
                    "salary",
                    decrypt(
                            employee.get("salary")
                                    .toString()
                    )
            );
        }

        // Decrypt emergency phone
        if (employee.get("emergency_phone") != null) {
            employee.put(
                    "emergency_phone",
                    decrypt(
                            employee.get("emergency_phone")
                                    .toString()
                    )
            );
        }

        return employee;
    }

    private String decrypt(
            String encryptedValue) {

        if (encryptedValue == null ||
                encryptedValue.isBlank()) {

            return encryptedValue;
        }

        try {

            String[] parts =
                    encryptedValue.split(":");

            if (parts.length != 2) {
                throw new DecryptionException(
                        "Invalid encrypted data"
                );
            }

            byte[] iv =
                    Base64.getDecoder()
                            .decode(parts[0]);

            byte[] encrypted =
                    Base64.getDecoder()
                            .decode(parts[1]);

            byte[] keyBytes =
                    MessageDigest
                            .getInstance("SHA-256")
                            .digest(
                                    secretKey.getBytes(
                                            StandardCharsets.UTF_8
                                    )
                            );

            SecretKeySpec key =
                    new SecretKeySpec(
                            keyBytes,
                            "AES"
                    );

            Cipher cipher =
                    Cipher.getInstance(
                            "AES/GCM/NoPadding"
                    );

            GCMParameterSpec parameterSpec =
                    new GCMParameterSpec(
                            128,
                            iv
                    );

            cipher.init(
                    Cipher.DECRYPT_MODE,
                    key,
                    parameterSpec
            );

            byte[] decrypted =
                    cipher.doFinal(encrypted);

            return new String(
                    decrypted,
                    StandardCharsets.UTF_8
            );

        } catch (DecryptionException e) {

            throw e;

        } catch (Exception e) {

            throw new DecryptionException(
                    "Failed to decrypt employee data"
            );
        }
    }
}