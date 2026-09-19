package com.amex.lumi.beam_ingestion.transform;

import com.amex.lumi.beam_ingestion.model.Address;
import com.amex.lumi.beam_ingestion.model.EmployeeRecord;
import com.amex.lumi.beam_ingestion.model.EmergencyContact;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.time.Instant;
import java.util.Arrays;

public class ParseFileFn extends DoFn<String, EmployeeRecord> {

    private final TupleTag<String> errorTag;

    private transient ObjectMapper objectMapper;

    public ParseFileFn(TupleTag<String> errorTag) {
        this.errorTag = errorTag;
    }



    @Setup
    public void setup() {

        objectMapper = new ObjectMapper();

        objectMapper.setPropertyNamingStrategy(
                PropertyNamingStrategies.SNAKE_CASE
        );
    }


    @ProcessElement
    public void processElement(ProcessContext context) {

        String fileLocation = context.element();

        try {

            String sourceCreationTime =
                    Instant.now().toString();

            String lowerCaseFile =
                    fileLocation.toLowerCase();

            // JSON
            if (lowerCaseFile.endsWith(".json")) {

                parseJson(
                        fileLocation,
                        sourceCreationTime,
                        context
                );

            }

            // CSV
            else if (lowerCaseFile.endsWith(".csv")) {

                parseCsv(
                        fileLocation,
                        sourceCreationTime,
                        context
                );



            }
//            else if (lowerCaseFile.endsWith(".xml")) {
//
//                parseXml(
//                        fileLocation,
//                        sourceCreationTime,
//                        context
//                );
//            }

            // Unsupported format
            else {

                context.output(
                        errorTag,
                        "UNSUPPORTED_FILE_FORMAT | "
                                + fileLocation
                );
            }

        } catch (Exception e) {

            context.output(
                    errorTag,
                    "PARSE_ERROR | "
                            + fileLocation
                            + " | "
                            + e.getMessage()
            );
        }
    }



    private void parseJson(
            String fileLocation,
            String sourceCreationTime,
            ProcessContext context
    ) throws Exception {

        JsonNode root =
                objectMapper.readTree(
                        new File(fileLocation)
                );

        if (!root.isArray()) {

            context.output(
                    errorTag,
                    "JSON_ERROR | Root must be an array"
            );

            return;
        }

        for (JsonNode node : root) {

            try {

                EmployeeRecord employee =
                        objectMapper.treeToValue(
                                node,
                                EmployeeRecord.class
                        );

                employee.setSourceCreationTime(
                        sourceCreationTime
                );

                context.output(employee);

            } catch (Exception e) {

                context.output(
                        errorTag,
                        "INVALID_JSON_RECORD | "
                                + node
                                + " | "
                                + e.getMessage()
                );
            }
        }
    }

    // ==================================================
    // CSV PARSER
    // ==================================================

    private void parseCsv(
            String fileLocation,
            String sourceCreationTime,
            ProcessContext context
    ) throws Exception {

        try (
                FileReader reader =
                        new FileReader(fileLocation);

                CSVParser csvParser =
                        CSVFormat.DEFAULT.builder()
                                .setHeader()
                                .setSkipHeaderRecord(true)
                                .setIgnoreEmptyLines(true)
                                .build()
                                .parse(reader)
        ) {

            for (CSVRecord record : csvParser) {

                try {

                    EmployeeRecord employee =
                            new EmployeeRecord();


                    employee.setEmployeeId(
                            getValue(record, "employee_id")
                    );

                    employee.setFirstName(
                            getValue(record, "first_name")
                    );

                    employee.setLastName(
                            getValue(record, "last_name")
                    );

                    employee.setEmail(
                            getValue(record, "email")
                    );

                    employee.setPhoneNumber(
                            getValue(record, "phone_number")
                    );

                    employee.setHireDate(
                            getValue(record, "hire_date")
                    );

                    employee.setDepartment(
                            getValue(record, "department")
                    );

                    employee.setJobTitle(
                            getValue(record, "job_title")
                    );

                    String salary =
                            getValue(record, "salary");

                        employee.setSalary(salary);

                    employee.setCurrency(
                            getValue(record, "currency")
                    );

                    employee.setEmploymentStatus(
                            getValue(record, "employment_status")
                    );

                    employee.setManagerId(
                            getValue(record, "manager_id")
                    );

                    String isActive =
                            getValue(record, "is_active");

                    if (!isActive.isBlank()) {

                        employee.setIsActive(
                                Boolean.parseBoolean(isActive)
                        );
                    }



                    String skills =
                            getValue(record, "skills");

                    if (!skills.isBlank()) {

                        employee.setSkills(
                                Arrays.stream(
                                                skills.split(",")
                                        )
                                        .map(String::trim)
                                        .toList()
                        );
                    }

                    // ----------------------------------
                    // Address
                    // ----------------------------------

                    Address address =
                            new Address();

                    address.setStreet(
                            getValue(record, "address_street")
                    );

                    address.setCity(
                            getValue(record, "address_city")
                    );

                    address.setState(
                            getValue(record, "address_state")
                    );

                    address.setPostalCode(
                            getValue(
                                    record,
                                    "address_postal_code"
                            )
                    );

                    address.setCountry(
                            getValue(record, "address_country")
                    );

                    employee.setAddress(address);



                    EmergencyContact contact =
                            new EmergencyContact();

                    contact.setName(
                            getValue(
                                    record,
                                    "emergency_name"
                            )
                    );

                    contact.setRelationship(
                            getValue(
                                    record,
                                    "emergency_relationship"
                            )
                    );

                    contact.setPhone(
                            getValue(
                                    record,
                                    "emergency_phone"
                            )
                    );

                    contact.setEmail(
                            getValue(
                                    record,
                                    "emergency_email"
                            )
                    );

                    employee.setEmergencyContact(
                            contact
                    );


                    employee.setSourceCreationTime(
                            sourceCreationTime
                    );

                    // Send valid parsed object
                    // to next Beam step
                    context.output(employee);

                } catch (Exception e) {

                    context.output(
                            errorTag,
                            "INVALID_CSV_RECORD | record="
                                    + record
                                    + " | "
                                    + e.getMessage()
                    );
                }
            }
        }
    }



    private String getValue(
            CSVRecord record,
            String columnName
    ) {

        if (!record.isMapped(columnName)) {
            return "";
        }

        String value =
                record.get(columnName);

        if (value == null) {
            return "";
        }

        return value.trim();
    }
}