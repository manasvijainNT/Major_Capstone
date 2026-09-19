package com.amex.lumi.ingestion.api.service;

import com.amex.lumi.ingestion.api.exception.EmployeeNotFoundException;
import com.amex.lumi.ingestion.api.repository.EmployeeRepository;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class EmployeeFetchService {

    private final EmployeeRepository employeeRepository;

    public EmployeeFetchService(
            EmployeeRepository employeeRepository) {

        this.employeeRepository = employeeRepository;
    }

    public Map<String, Object> getEmployee(
            String employeeId) {

        try {

            return employeeRepository
                    .findEmployeeById(employeeId);

        } catch (EmptyResultDataAccessException e) {

            throw new EmployeeNotFoundException(
                    "Employee not found with ID: "
                            + employeeId
            );
        }
    }
}