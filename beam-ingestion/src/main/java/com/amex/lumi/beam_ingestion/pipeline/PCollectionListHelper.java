package com.amex.lumi.beam_ingestion.pipeline;

import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.io.TextIO;

public class PCollectionListHelper {

    private PCollectionListHelper() {
    }

    public static void writeErrors(
            PCollection<String> parseErrors,
            PCollection<String> validationErrors,
            PCollection<String> postgresErrors,
            String errorFile
    ) {

        PCollectionList<String> allErrors =
                PCollectionList.of(parseErrors)
                        .and(validationErrors)
                        .and(postgresErrors);

        allErrors
                .apply(
                        "CombineAllErrors",
                        Flatten.pCollections()
                )
                .apply(
                        "WriteErrorsToFile",
                        TextIO.write()
                                .to(errorFile)
                                .withSuffix(".txt")
                                .withoutSharding()
                );
    }
}