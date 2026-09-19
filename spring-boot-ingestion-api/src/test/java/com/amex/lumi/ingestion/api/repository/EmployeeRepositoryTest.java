package com.amex.lumi.ingestion.api.repository;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class EmployeeRepositoryTest {

    @Test
    void testFindEmployeeById() {

        JdbcTemplate jdbcTemplate =
                mock(JdbcTemplate.class);

        EmployeeRepository repository =
                new EmployeeRepository(
                        jdbcTemplate
                );

        Map<String, Object> employee =
                Map.of(
                        "employee_id", "EMP001",
                        "first_name", "Manasvi",
                        "last_name", "Jain"
                );

        when(
                jdbcTemplate.queryForMap(
                        anyString(),
                        eq("EMP001")
                )
        ).thenReturn(employee);

        Map<String, Object> result =
                repository.findEmployeeById("EMP001");

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

        JdbcTemplate jdbcTemplate =
                mock(JdbcTemplate.class);

        EmployeeRepository repository =
                new EmployeeRepository(
                        jdbcTemplate
                );

        when(
                jdbcTemplate.queryForMap(
                        anyString(),
                        eq("EMP999")
                )
        ).thenThrow(
                new org.springframework.dao.EmptyResultDataAccessException(1)
        );

        assertThrows(
                org.springframework.dao.EmptyResultDataAccessException.class,
                () -> repository.findEmployeeById("EMP999")
        );
    }
}