package com.amex.lumi.beam_ingestion.transform;

import com.amex.lumi.beam_ingestion.model.Address;
import com.amex.lumi.beam_ingestion.model.EmployeeRecord;
import com.amex.lumi.beam_ingestion.model.EmergencyContact;
import org.apache.beam.sdk.transforms.DoFn;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

public class EncryptionFn extends DoFn<EmployeeRecord, EmployeeRecord> {

    private final String secretKey;

    public EncryptionFn() {

        this.secretKey = System.getenv("LUMI_ENCRYPTION_SECRET_KEY");
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {

        EmployeeRecord oldEmployee = context.element();

        EmployeeRecord employee = new EmployeeRecord();

        employee.setEmployeeId(oldEmployee.getEmployeeId());
        employee.setFirstName(oldEmployee.getFirstName());
        employee.setLastName(oldEmployee.getLastName());
        employee.setEmail(oldEmployee.getEmail());

        // Encrypt phone number
        employee.setPhoneNumber(
                encrypt(oldEmployee.getPhoneNumber())
        );

        employee.setHireDate(oldEmployee.getHireDate());
        employee.setDepartment(oldEmployee.getDepartment());
        employee.setJobTitle(oldEmployee.getJobTitle());

        // Encrypt salary
        employee.setSalary(
                encrypt(oldEmployee.getSalary())
        );

        employee.setCurrency(oldEmployee.getCurrency());
        employee.setEmploymentStatus(oldEmployee.getEmploymentStatus());
        employee.setManagerId(oldEmployee.getManagerId());
        employee.setIsActive(oldEmployee.getIsActive());
        employee.setSkills(oldEmployee.getSkills());

        if (oldEmployee.getAddress() != null) {

            Address oldAddress = oldEmployee.getAddress();

            Address address = new Address();

            address.setStreet(oldAddress.getStreet());
            address.setCity(oldAddress.getCity());
            address.setState(oldAddress.getState());
            address.setPostalCode(oldAddress.getPostalCode());
            address.setCountry(oldAddress.getCountry());

            employee.setAddress(address);
        }

        if (oldEmployee.getEmergencyContact() != null) {

            EmergencyContact oldContact =
                    oldEmployee.getEmergencyContact();

            EmergencyContact contact =
                    new EmergencyContact();

            contact.setName(oldContact.getName());
            contact.setRelationship(oldContact.getRelationship());

            // Encrypt emergency contact phone
            contact.setPhone(
                    encrypt(oldContact.getPhone())
            );

            contact.setEmail(oldContact.getEmail());

            employee.setEmergencyContact(contact);
        }

        employee.setExecutionId(
                oldEmployee.getExecutionId()
        );

        employee.setIngestionTimestamp(
                oldEmployee.getIngestionTimestamp()
        );

        employee.setSourceCreationTime(
                oldEmployee.getSourceCreationTime()
        );

        context.output(employee);
    }

    private String encrypt(String value) throws Exception {

        if (value == null || value.isBlank()) {
            return value;
        }

        // Create 256-bit AES key
        byte[] keyBytes =
                MessageDigest
                        .getInstance("SHA-256")
                        .digest(
                                secretKey.getBytes(
                                        StandardCharsets.UTF_8
                                )
                        );

        SecretKeySpec key =
                new SecretKeySpec(keyBytes, "AES");

        byte[] iv = new byte[12];

        new SecureRandom().nextBytes(iv);

        Cipher cipher =
                Cipher.getInstance(
                        "AES/GCM/NoPadding"
                );

        GCMParameterSpec parameterSpec =
                new GCMParameterSpec(128, iv);

        cipher.init(
                Cipher.ENCRYPT_MODE,
                key,
                parameterSpec
        );

        byte[] encrypted =
                cipher.doFinal(
                        value.getBytes(
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