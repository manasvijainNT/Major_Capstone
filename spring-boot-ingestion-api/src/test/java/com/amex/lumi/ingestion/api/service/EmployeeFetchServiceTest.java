package com.amex.lumi.ingestion.api.service;

import com.amex.lumi.ingestion.api.exception.EmployeeNotFoundException;
import com.amex.lumi.ingestion.api.repository.EmployeeRepository;
import org.junit.jupiter.api.Test;
import org.springframework.dao.EmptyResultDataAccessException;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class EmployeeFetchServiceTest {

    @Test
    void testGetEmployee() {

        EmployeeRepository employeeRepository =
                mock(EmployeeRepository.class);

        EmployeeFetchService service =
                new EmployeeFetchService(
                        employeeRepository
                );

        Map<String, Object> employee = Map.of(
                "employee_id", "EMP001",
                "first_name", "Manasvi",
                "last_name", "Jain",
                "phone_number", "encrypted-phone",
                "salary", "encrypted-salary"
        );

        when(
                employeeRepository
                        .findEmployeeById("EMP001")
        ).thenReturn(employee);

        Map<String, Object> result =
                service.getEmployee("EMP001");

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
                "Jain",
                result.get("last_name")
        );
    }

    @Test
    void testEmployeeNotFound() {

        EmployeeRepository employeeRepository =
                mock(EmployeeRepository.class);

        EmployeeFetchService service =
                new EmployeeFetchService(
                        employeeRepository
                );

        when(
                employeeRepository
                        .findEmployeeById("EMP999")
        ).thenThrow(
                new EmptyResultDataAccessException(1)
        );

        EmployeeNotFoundException exception =
                assertThrows(
                        EmployeeNotFoundException.class,
                        () -> service.getEmployee("EMP999")
                );

        assertEquals(
                "Employee not found with ID: EMP999",
                exception.getMessage()
        );
    }
}