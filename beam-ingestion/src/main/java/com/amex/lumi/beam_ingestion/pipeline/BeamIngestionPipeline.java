package com.amex.lumi.beam_ingestion.pipeline;

import com.amex.lumi.beam_ingestion.model.EmployeeRecord;
import com.amex.lumi.beam_ingestion.transform.*;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BeamIngestionPipeline {

    private static final Logger log = LoggerFactory.getLogger(BeamIngestionPipeline.class);

    public static void main(String[] args) {

        log.info("Beam ingestion started");

        String fileLocation = getArgument(
                args,
                "--file_location"
        );

        String executionId = getArgument(
                args,
                "--execution_id"
        );

        long expectedCount = Long.parseLong(
                getArgument(args, "--expected_count")
        );

        String errorFile = getArgument(
                args,
                "--error_file"
        );

        log.info("File location: {}", fileLocation);
        log.info("Execution ID: {}", executionId);



        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);

        TupleTag<String> parseErrorTag =
                new TupleTag<String>() {};

        TupleTag<String> validationErrorTag =
                new TupleTag<String>() {};

        TupleTag<String> postgresErrorTag =
                new TupleTag<String>() {};

        TupleTag<EmployeeRecord> parsedEmployeeTag =
                new TupleTag<EmployeeRecord>() {};

        TupleTag<EmployeeRecord> validEmployeeTag =
                new TupleTag<EmployeeRecord>() {};

        TupleTag<Void> postgresSuccessTag =
                new TupleTag<Void>() {};

        PCollection<String> inputFile =
                pipeline.apply(
                        "CreateInputFile",
                        Create.of(fileLocation)
                );

        PCollectionTuple parsedRecords =
                inputFile.apply(
                        "ParseFile",
                        ParDo.of(
                                new ParseFileFn(parseErrorTag)
                        ).withOutputTags(
                                parsedEmployeeTag,
                                TupleTagList.of(parseErrorTag)
                        )
                );

        PCollection<EmployeeRecord> employees =
                parsedRecords.get(
                        parsedEmployeeTag
                );

        PCollection<String> parseErrors =
                parsedRecords.get(
                        parseErrorTag
                );

        PCollection<EmployeeRecord> normalizedEmployees =
                employees.apply(
                        "NormalizeEmployee",
                        ParDo.of(
                                new NormalizeEmployeeFn()
                        )
                );

        PCollectionTuple validatedRecords =
                normalizedEmployees.apply(
                        "ValidateEmployee",
                        ParDo.of(
                                new ValidateEmployeeFn(
                                        validationErrorTag
                                )
                        ).withOutputTags(
                                validEmployeeTag,
                                TupleTagList.of(
                                        validationErrorTag
                                )
                        )
                );

        PCollection<EmployeeRecord> validEmployees = validatedRecords.get(validEmployeeTag);

        PCollection<String> validationErrors = validatedRecords.get(validationErrorTag);

        PCollection<Long> validCount =
                validEmployees.apply("Count Valid Records", Count.globally());

        PCollection<Long> validationErrorCount =
                validationErrors.apply("Count Validation Errors", Count.globally());

        PCollection<Long> parseErrorCount =
                parseErrors.apply("Count parse Errors", Count.globally());

        PCollection<Long> invalidCount =
                PCollectionList.of(validationErrorCount)
                        .and(parseErrorCount)
                        .apply("Combine Invalid Counts", Flatten.pCollections())
                        .apply("Calculate Invalid Record Count", Sum.longsGlobally());

        PCollection<Long> actualRecordCount =
                PCollectionList.of(validCount)
                        .and(invalidCount)
                        .apply("Combine Record Counts", Flatten.pCollections())
                        .apply("Calculate Actual Record Count", Sum.longsGlobally());

        PCollectionView<Long> validCountView = validCount.apply(View.asSingleton());

        PCollectionView<Long> invalidCountView = invalidCount.apply(View.asSingleton());

        actualRecordCount.apply(
                "Log Ingestion Counts",
                ParDo.of(
                        new LogIngestionCountsFn(
                                executionId,
                                validCountView,
                                invalidCountView,
                                expectedCount
                        )
                ).withSideInputs(
                        validCountView,
                        invalidCountView
                )
        );


        PCollection<EmployeeRecord> encryptedEmployees =
                validEmployees.apply(
                        "Encrypt Sensitive Fields",
                        ParDo.of(new EncryptionFn())
                );


        PCollection<EmployeeRecord> employeesWithMetadata =
                encryptedEmployees.apply(
                        "AddMetadata",
                        ParDo.of(
                                new AddMetadataFn(executionId)
                        )
                );


        PCollectionTuple databaseResult =
                employeesWithMetadata.apply(
                        "WriteToPostgres",
                        ParDo.of(
                                new WriteToPostgresFn(
                                        postgresErrorTag
                                )
                        ).withOutputTags(
                                postgresSuccessTag,
                                TupleTagList.of(
                                        postgresErrorTag
                                )
                        )
                );

        PCollection<String> postgresErrors =
                databaseResult.get(
                        postgresErrorTag
                );



        PCollectionListHelper.writeErrors(
                parseErrors,
                validationErrors,
                postgresErrors,
                errorFile
        );

        pipeline.run().waitUntilFinish();

        System.out.println("----Beam ingestion pipeline completed----");

    }

    private static long readExpectedCount(
            String controlFileLocation
    ) {

        try {

            java.nio.file.Path path =
                    java.nio.file.Paths.get(controlFileLocation);

            for (String line :
                    java.nio.file.Files.readAllLines(path)) {

                line = line.trim();

                if (line.startsWith("record_count=")) {

                    return Long.parseLong(
                            line.split("=", 2)[1].trim()
                    );
                }
            }

            throw new IllegalArgumentException(
                    "record_count not found in control file"
            );

        } catch (Exception e) {

            throw new RuntimeException(
                    "Unable to read control file: "
                            + controlFileLocation,
                    e
            );
        }
    }

    private static String getArgument(
            String[] args,
            String argumentName
    ) {

        for (int i = 0; i < args.length - 1; i++) {

            if (args[i].equals(argumentName)) {

                return args[i + 1];
            }
        }

        throw new IllegalArgumentException(
                "Missing argument: " + argumentName
        );
    }
}