package com.amex.lumi.beam_ingestion.transform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

public class LogIngestionCountsFn extends DoFn<Long, Void> {

    private final String executionId;
    private final PCollectionView<Long> validCountView;
    private final PCollectionView<Long> invalidCountView;
    private final long expectedCount;

    public LogIngestionCountsFn(
            String executionId,
            PCollectionView<Long> validCountView,
            PCollectionView<Long> invalidCountView,
            long expectedCount
    ) {
        this.executionId = executionId;
        this.validCountView = validCountView;
        this.invalidCountView = invalidCountView;
        this.expectedCount = expectedCount;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {

        Long actualCount = context.element();

        Long validCount =
                context.sideInput(validCountView);

        Long invalidCount =
                context.sideInput(invalidCountView);


        System.out.println("INGESTION COUNT SUMMARY");

        System.out.println(
                "Execution ID: " + executionId
        );

        System.out.println(
                "Expected Count: " + expectedCount
        );

        System.out.println(
                "Valid Count: " + validCount
        );

        System.out.println(
                "Invalid Count: " + invalidCount
        );

        System.out.println(
                "Actual Parsed Count: " + actualCount
        );

    }
}