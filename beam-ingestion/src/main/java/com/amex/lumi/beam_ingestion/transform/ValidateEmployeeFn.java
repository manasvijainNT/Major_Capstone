package com.amex.lumi.beam_ingestion.transform;

import com.amex.lumi.beam_ingestion.model.EmployeeRecord;
import com.amex.lumi.beam_ingestion.validation.EmployeeValidator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ValidateEmployeeFn
        extends DoFn<EmployeeRecord, EmployeeRecord> {

    private static final Logger log = LoggerFactory.getLogger(ValidateEmployeeFn.class);

    private final TupleTag<String> errorTag;

    public ValidateEmployeeFn(TupleTag<String> errorTag) {
        this.errorTag = errorTag;
    }


    @ProcessElement
    public void processElement(ProcessContext context) {

        EmployeeRecord employee = context.element();

        List<String> errors =
                EmployeeValidator.validate(employee);

        if (errors.isEmpty()) {
            log.info("Employee validation successfully. Employee ID : {}",
                    employee.getEmployeeId()
            );

            // Valid record
            context.output(employee);

        } else {

            // Invalid record
            String errorMessage =
                    "VALIDATION_ERROR"
                            + " | employee_id="
                            + employee.getEmployeeId()
                            + " | "
                            + String.join(
                            "; ",
                            errors
                    );

            context.output(
                    errorTag,
                    errorMessage
            );
        }
    }
}