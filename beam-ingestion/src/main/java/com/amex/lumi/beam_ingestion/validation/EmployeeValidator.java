package com.amex.lumi.beam_ingestion.validation;
import com.amex.lumi.beam_ingestion.model.Address;
import com.amex.lumi.beam_ingestion.model.EmployeeRecord;
import com.amex.lumi.beam_ingestion.model.EmergencyContact;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
public class EmployeeValidator {
    private EmployeeValidator() {
    }
    public static List<String> validate(EmployeeRecord employee) {
        List<String> errors = new ArrayList<>();
        if (employee == null) {
            errors.add("Employee record is null");
            return errors;
        }
        validateEmployeeId(employee, errors);
        validateFirstName(employee, errors);
        validateLastName(employee, errors);
        validateEmail(employee, errors);
        validatePhoneNumber(employee, errors);
        validateHireDate(employee, errors);
        validateDepartment(employee, errors);
        validateJobTitle(employee, errors);
        validateSalary(employee, errors);
        validateCurrency(employee, errors);
        validateEmploymentStatus(employee, errors);
        validateManagerId(employee, errors);
        validateIsActive(employee, errors);
        validateSkills(employee, errors);
        validateAddress(employee, errors);
        validateEmergencyContact(employee, errors);
        return errors;
    }

    private static void validateEmployeeId(
            EmployeeRecord employee,
            List<String> errors) {
        String value = employee.getEmployeeId();
        if (value == null || value.isBlank()) {
            errors.add("employee_id is required");
            return;
        }
        if (value.length() != 7) {
            errors.add(
                    "employee_id must be exactly 7 characters"
            );
        }
    }

    private static void validateFirstName(
            EmployeeRecord employee,
            List<String> errors) {
        String value = employee.getFirstName();
        if (value == null || value.isBlank()) {
            errors.add("first_name is required");
            return;
        }
        if (value.length() < 3 || value.length() > 15) {
            errors.add(
                    "first_name must be between 3 and 15 characters"
            );
        }
    }

    private static void validateLastName(
            EmployeeRecord employee,
            List<String> errors) {
        String value = employee.getLastName();
        // PO says minimum is 0, so blank is allowed.
        if (value == null) {
            return;
        }
        if (value.length() > 15) {
            errors.add(
                    "last_name must not exceed 15 characters"
            );
        }
    }

    private static void validateEmail(
            EmployeeRecord employee,
            List<String> errors) {
        String value = employee.getEmail();
        if (value == null || value.isBlank()) {
            errors.add("email is required");
            return;
        }
        if (value.length() < 13 || value.length() > 30) {
            errors.add(
                    "email must be between 13 and 30 characters"
            );
        }
        if (!value.matches(
                "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$")) {
            errors.add(
                    "email has invalid format"
            );
        }
    }

    private static void validatePhoneNumber(
            EmployeeRecord employee,
            List<String> errors) {
        String value = employee.getPhoneNumber();
        if (value == null || value.isBlank()) {
            errors.add("phone_number is required");
            return;
        }
        if (value.length() != 10) {
            errors.add(
                    "phone_number must be exactly 10 characters"
            );
        }
        if (!value.matches("\\d+")) {
            errors.add(
                    "phone_number must contain only digits"
            );
        }
    }

    private static void validateHireDate(
            EmployeeRecord employee,
            List<String> errors) {
        String value = employee.getHireDate();
        if (value == null || value.isBlank()) {
            errors.add("hire_date is required");
            return;
        }
        if (value.length() != 10) {
            errors.add(
                    "hire_date must be exactly 10 characters"
            );
            return;
        }
        try {
            LocalDate.parse(value);
        } catch (Exception e) {
            errors.add(
                    "hire_date must be in yyyy-MM-dd format"
            );
        }
    }

    private static void validateDepartment(
            EmployeeRecord employee,
            List<String> errors) {
        String value = employee.getDepartment();
        if (value == null) {
            return;
        }
        if (value.length() > 20) {
            errors.add(
                    "department must not exceed 20 characters"
            );
        }
    }

    private static void validateJobTitle(
            EmployeeRecord employee,
            List<String> errors) {
        String value = employee.getJobTitle();
        if (value == null) {
            return;
        }
        if (value.length() > 30) {
            errors.add(
                    "job_title must not exceed 30 characters"
            );
        }
    }

    private static void validateSalary(
            EmployeeRecord employee,
            List<String> errors) {

        String value = employee.getSalary();

        if (value == null || value.isBlank()) {
            errors.add("salary is required");
            return;
        }

        try {

            java.math.BigDecimal salary =
                    new java.math.BigDecimal(value);

            if (salary.signum() < 0) {
                errors.add(
                        "salary cannot be negative"
                );
            }

        } catch (NumberFormatException e) {

            errors.add(
                    "salary must be a valid number"
            );
        }
    }

    private static void validateCurrency(
            EmployeeRecord employee,
            List<String> errors) {
        String value = employee.getCurrency();
        if (value == null || value.isBlank()) {
            errors.add("currency is required");
            return;
        }
        if (value.length() != 3) {
            errors.add(
                    "currency must be exactly 3 characters"
            );
        }
    }

    private static void validateEmploymentStatus(
            EmployeeRecord employee,
            List<String> errors) {
        String value = employee.getEmploymentStatus();
        if (value == null || value.isBlank()) {
            errors.add(
                    "employment_status is required"
            );
            return;
        }
        if (value.length() < 3 || value.length() > 13) {
            errors.add(
                    "employment_status must be between 3 and 13 characters"
            );
        }
    }

    private static void validateManagerId(
            EmployeeRecord employee,
            List<String> errors) {
        String value = employee.getManagerId();
        if (value == null || value.isBlank()) {
            errors.add("manager_id is required");
            return;
        }
        if (value.length() != 7) {
            errors.add(
                    "manager_id must be exactly 7 characters"
            );
        }
    }

    private static void validateIsActive(
            EmployeeRecord employee,
            List<String> errors) {
        if (employee.getIsActive() == null) {
            errors.add("is_active is required");
        }
    }

    private static void validateSkills(
            EmployeeRecord employee,
            List<String> errors) {
        if (employee.getSkills() == null) {
            errors.add("skills is required");
            return;
        }
        int totalLength = 0;
        for (String skill : employee.getSkills()) {
            if (skill == null || skill.isBlank()) {
                errors.add(
                        "skills cannot contain empty values"
                );
            } else {
                totalLength += skill.length();
            }
        }
        if (totalLength > 100) {
            errors.add(
                    "skills total length must not exceed 100 characters"
            );
        }
    }

    private static void validateAddress(
            EmployeeRecord employee,
            List<String> errors) {
        Address address = employee.getAddress();
        if (address == null) {
            errors.add("address is required");
            return;
        }
        validateAddressField(
                "address.street",
                address.getStreet(),
                255,
                errors
        );
        validateAddressField(
                "address.city",
                address.getCity(),
                100,
                errors
        );
        validateAddressField(
                "address.state",
                address.getState(),
                100,
                errors
        );
        validateAddressField(
                "address.postal_code",
                address.getPostalCode(),
                20,
                errors
        );
        validateAddressField(
                "address.country",
                address.getCountry(),
                100,
                errors
        );
    }

    private static void validateAddressField(
            String fieldName,
            String value,
            int maxLength,
            List<String> errors) {
        if (value == null || value.isBlank()) {
            errors.add(
                    fieldName + " is required"
            );
            return;
        }
        if (value.length() > maxLength) {
            errors.add(
                    fieldName
                            + " must not exceed "
                            + maxLength
                            + " characters"
            );
        }
    }

    private static void validateEmergencyContact(
            EmployeeRecord employee,
            List<String> errors) {
        EmergencyContact contact =
                employee.getEmergencyContact();
        if (contact == null) {
            errors.add(
                    "emergency_contact is required"
            );
            return;
        }
        if (contact.getName() == null
                || contact.getName().isBlank()) {
            errors.add(
                    "emergency_contact.name is required"
            );
        }
        if (contact.getRelationship() == null
                || contact.getRelationship().isBlank()) {
            errors.add(
                    "emergency_contact.relationship is required"
            );
        }
        if (contact.getPhone() == null
                || contact.getPhone().isBlank()) {
            errors.add(
                    "emergency_contact.phone is required"
            );
        }
        if (contact.getEmail() == null
                || contact.getEmail().isBlank()) {
            errors.add(
                    "emergency_contact.email is required"
            );
        } else if (!contact.getEmail().matches(
                "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$")) {
            errors.add(
                    "emergency_contact.email has invalid format"
            );
        }
    }
}