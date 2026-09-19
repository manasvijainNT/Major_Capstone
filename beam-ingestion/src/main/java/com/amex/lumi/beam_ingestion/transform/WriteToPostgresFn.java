package com.amex.lumi.beam_ingestion.transform;

import com.amex.lumi.beam_ingestion.config.DatabaseConfig;
import com.amex.lumi.beam_ingestion.config.DatabaseInitializer;
import com.amex.lumi.beam_ingestion.model.Address;
import com.amex.lumi.beam_ingestion.model.EmployeeRecord;
import com.amex.lumi.beam_ingestion.model.EmergencyContact;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;

public class WriteToPostgresFn extends DoFn<EmployeeRecord, Void> {

    private final TupleTag<String> errorTag;

    private transient Connection connection;

    private static final String SQL = """
            INSERT INTO employee (
                employee_id,
                first_name,
                last_name,
                email,
                phone_number,
                hire_date,
                department,
                job_title,
                salary,
                currency,
                employment_status,
                manager_id,
                is_active,
                skills,
                address_street,
                address_city,
                address_state,
                address_postal_code,
                address_country,
                emergency_name,
                emergency_relationship,
                emergency_phone,
                emergency_email,
                ingestion_timestamp,
                execution_id,
                source_creation_time
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?
            )
            """;

    public WriteToPostgresFn(
            TupleTag<String> errorTag
    ) {
        this.errorTag = errorTag;
    }

    @Setup
    public void setup() throws Exception {

        System.out.println(
                "Initializing PostgreSQL connection"
        );

        DatabaseInitializer.initialize();

        connection = DriverManager.getConnection(
                DatabaseConfig.getJdbcUrl(),
                DatabaseConfig.getUsername(),
                DatabaseConfig.getPassword()
        );

        System.out.println(
                "PostgreSQL connection initialized"
        );
    }

    @ProcessElement
    public void processElement(
            ProcessContext context
    ) {

        EmployeeRecord employee = context.element();

        try (
                PreparedStatement statement =
                        connection.prepareStatement(SQL)
        ) {

            Address address =
                    employee.getAddress();

            EmergencyContact contact =
                    employee.getEmergencyContact();

            statement.setString(
                    1,
                    employee.getEmployeeId()
            );

            statement.setString(
                    2,
                    employee.getFirstName()
            );

            statement.setString(
                    3,
                    employee.getLastName()
            );

            statement.setString(
                    4,
                    employee.getEmail()
            );

            statement.setString(
                    5,
                    employee.getPhoneNumber()
            );

            if (employee.getHireDate() != null
                    && !employee.getHireDate().isBlank()
                    && !employee.getHireDate().equals(" ")) {

                statement.setDate(
                        6,
                        java.sql.Date.valueOf(
                                employee.getHireDate().trim()
                        )
                );

            } else {

                statement.setNull(
                        6,
                        java.sql.Types.DATE
                );
            }

            statement.setString(
                    7,
                    employee.getDepartment()
            );

            statement.setString(
                    8,
                    employee.getJobTitle()
            );

            if (employee.getSalary() != null) {

                statement.setString(
                        9,
                        employee.getSalary()
                );

            } else {

                statement.setNull(
                        9,
                        java.sql.Types.NUMERIC
                );
            }

            statement.setString(
                    10,
                    employee.getCurrency()
            );

            statement.setString(
                    11,
                    employee.getEmploymentStatus()
            );


            statement.setString(
                    12,
                    employee.getManagerId()
            );

            if (employee.getIsActive() != null) {

                statement.setBoolean(
                        13,
                        employee.getIsActive()
                );

            } else {

                statement.setNull(
                        13,
                        java.sql.Types.BOOLEAN
                );
            }

            String skills = "";

            if (employee.getSkills() != null
                    && !employee.getSkills().isEmpty()) {

                skills = String.join(
                        ",",
                        employee.getSkills()
                );
            }

            statement.setString(
                    14,
                    skills
            );



            if (address != null) {

                statement.setString(
                        15,
                        address.getStreet()
                );

                statement.setString(
                        16,
                        address.getCity()
                );

                statement.setString(
                        17,
                        address.getState()
                );

                statement.setString(
                        18,
                        address.getPostalCode()
                );

                statement.setString(
                        19,
                        address.getCountry()
                );

            } else {

                statement.setString(15, " ");
                statement.setString(16, " ");
                statement.setString(17, " ");
                statement.setString(18, " ");
                statement.setString(19, " ");
            }


            if (contact != null) {

                statement.setString(
                        20,
                        contact.getName()
                );

                statement.setString(
                        21,
                        contact.getRelationship()
                );

                statement.setString(
                        22,
                        contact.getPhone()
                );

                statement.setString(
                        23,
                        contact.getEmail()
                );

            } else {

                statement.setString(20, " ");
                statement.setString(21, " ");
                statement.setString(22, " ");
                statement.setString(23, " ");
            }



            statement.setTimestamp(
                    24,
                    java.sql.Timestamp.from(
                            Instant.now()
                    )
            );

            statement.setString(
                    25,
                    employee.getExecutionId()
            );

            if (employee.getSourceCreationTime() != null
                    && !employee.getSourceCreationTime().isBlank()
                    && !employee.getSourceCreationTime().equals(" ")) {

                statement.setTimestamp(
                        26,
                        java.sql.Timestamp.from(
                                Instant.parse(
                                        employee.getSourceCreationTime()
                                )
                        )
                );

            } else {

                statement.setNull(
                        26,
                        java.sql.Types.TIMESTAMP
                );
            }



            statement.executeUpdate();

        } catch (Exception e) {

            String errorMessage =
                    "POSTGRES_LOAD_ERROR"
                            + " | employee_id="
                            + employee.getEmployeeId()
                            + " | "
                            + e.getMessage();

            context.output(
                    errorTag,
                    errorMessage
            );
        }
    }


    @Teardown
    public void teardown() {

        System.out.println(
                "Closing PostgreSQL connection"
        );

        try {

            if (connection != null
                    && !connection.isClosed()) {

                connection.close();
            }

        } catch (SQLException e) {

            System.out.println(
                    "Error closing PostgreSQL connection: "
                            + e.getMessage()
            );
        }
    }
}