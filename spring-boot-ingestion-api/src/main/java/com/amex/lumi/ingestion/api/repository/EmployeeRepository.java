package com.amex.lumi.ingestion.api.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Repository
public class EmployeeRepository {

    private final JdbcTemplate jdbcTemplate;

    public EmployeeRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Map<String, Object> findEmployeeById(
            String employeeId) {

        String sql = """
                SELECT *
                FROM employee
                WHERE employee_id = ?
                """;

        return jdbcTemplate.queryForMap(
                sql,
                employeeId
        );
    }
}