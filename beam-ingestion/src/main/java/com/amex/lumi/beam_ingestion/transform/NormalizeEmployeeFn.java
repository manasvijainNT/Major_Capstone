package com.amex.lumi.beam_ingestion.transform;

import com.amex.lumi.beam_ingestion.model.Address;
import com.amex.lumi.beam_ingestion.model.EmployeeRecord;
import com.amex.lumi.beam_ingestion.model.EmergencyContact;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.ArrayList;

public class NormalizeEmployeeFn extends DoFn<EmployeeRecord, EmployeeRecord> {

    @ProcessElement
    public void processElement(ProcessContext context) {

        EmployeeRecord original = context.element();

        // Create a NEW object instead of modifying input
        EmployeeRecord employee = new EmployeeRecord();

        employee.setEmployeeId(clean(original.getEmployeeId()));
        employee.setFirstName(clean(original.getFirstName()));
        employee.setLastName(clean(original.getLastName()));
        employee.setEmail(clean(original.getEmail()));
        employee.setPhoneNumber(clean(original.getPhoneNumber()));
        employee.setHireDate(clean(original.getHireDate()));
        employee.setDepartment(clean(original.getDepartment()));
        employee.setJobTitle(clean(original.getJobTitle()));
        employee.setSalary(original.getSalary());
        employee.setCurrency(clean(original.getCurrency()));
        employee.setEmploymentStatus(clean(original.getEmploymentStatus()));
        employee.setManagerId(clean(original.getManagerId()));
        employee.setIsActive(original.getIsActive());

        // Skills
        if (original.getSkills() != null) {
            employee.setSkills(
                    new ArrayList<>(original.getSkills())
            );
        } else {
            employee.setSkills(new ArrayList<>());
        }

        // Address
        if (original.getAddress() != null) {

            Address originalAddress = original.getAddress();

            Address address = new Address();

            address.setStreet(
                    clean(originalAddress.getStreet())
            );

            address.setCity(
                    clean(originalAddress.getCity())
            );

            address.setState(
                    clean(originalAddress.getState())
            );

            address.setPostalCode(
                    clean(originalAddress.getPostalCode())
            );

            address.setCountry(
                    clean(originalAddress.getCountry())
            );

            employee.setAddress(address);

        } else {
            employee.setAddress(new Address());
        }

        // Emergency Contact
        if (original.getEmergencyContact() != null) {

            EmergencyContact originalContact =
                    original.getEmergencyContact();

            EmergencyContact contact =
                    new EmergencyContact();

            contact.setName(
                    clean(originalContact.getName())
            );

            contact.setRelationship(
                    clean(originalContact.getRelationship())
            );

            contact.setPhone(
                    clean(originalContact.getPhone())
            );

            contact.setEmail(
                    clean(originalContact.getEmail())
            );

            employee.setEmergencyContact(contact);

        } else {
            employee.setEmergencyContact(
                    new EmergencyContact()
            );
        }

        // Keep metadata if already present
        employee.setIngestionTimestamp(
                original.getIngestionTimestamp()
        );

        employee.setExecutionId(
                original.getExecutionId()
        );

        employee.setSourceCreationTime(
                original.getSourceCreationTime()
        );

        // Output NEW object
        context.output(employee);
    }

    private String clean(String value) {

        if (value == null || value.trim().isEmpty()) {
            return " ";
        }

        return value.trim();
    }
}