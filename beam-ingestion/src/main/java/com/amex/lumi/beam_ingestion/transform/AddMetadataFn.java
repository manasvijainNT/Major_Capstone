package com.amex.lumi.beam_ingestion.transform;

import com.amex.lumi.beam_ingestion.model.Address;
import com.amex.lumi.beam_ingestion.model.EmployeeRecord;
import com.amex.lumi.beam_ingestion.model.EmergencyContact;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.ArrayList;

public class AddMetadataFn extends DoFn<EmployeeRecord, EmployeeRecord> {

    private final String executionId;

    public AddMetadataFn(String executionId) {
        this.executionId = executionId;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {

        EmployeeRecord original = context.element();

        EmployeeRecord employee = new EmployeeRecord();

        employee.setEmployeeId(original.getEmployeeId());
        employee.setFirstName(original.getFirstName());
        employee.setLastName(original.getLastName());
        employee.setEmail(original.getEmail());
        employee.setPhoneNumber(original.getPhoneNumber());
        employee.setHireDate(original.getHireDate());
        employee.setDepartment(original.getDepartment());
        employee.setJobTitle(original.getJobTitle());
        employee.setSalary(original.getSalary());
        employee.setCurrency(original.getCurrency());
        employee.setEmploymentStatus(original.getEmploymentStatus());
        employee.setManagerId(original.getManagerId());
        employee.setIsActive(original.getIsActive());


        if (original.getSkills() != null) {
            employee.setSkills(
                    new ArrayList<>(original.getSkills())
            );
        }

        if (original.getAddress() != null) {

            Address originalAddress = original.getAddress();

            Address address = new Address();

            address.setStreet(
                    originalAddress.getStreet()
            );

            address.setCity(
                    originalAddress.getCity()
            );

            address.setState(
                    originalAddress.getState()
            );

            address.setPostalCode(
                    originalAddress.getPostalCode()
            );

            address.setCountry(
                    originalAddress.getCountry()
            );

            employee.setAddress(address);
        }


        if (original.getEmergencyContact() != null) {

            EmergencyContact originalContact =
                    original.getEmergencyContact();

            EmergencyContact contact =
                    new EmergencyContact();

            contact.setName(
                    originalContact.getName()
            );

            contact.setRelationship(
                    originalContact.getRelationship()
            );

            contact.setPhone(
                    originalContact.getPhone()
            );

            contact.setEmail(
                    originalContact.getEmail()
            );

            employee.setEmergencyContact(contact);
        }

        employee.setIngestionTimestamp(
                original.getIngestionTimestamp()
        );

        employee.setSourceCreationTime(
                original.getSourceCreationTime()
        );

        employee.setExecutionId(executionId);

        context.output(employee);
    }
}