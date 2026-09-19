package com.amex.lumi.ingestion.api.exception;

public class InvalidInputFileException extends RuntimeException {

    public InvalidInputFileException(String message) {
        super(message);
    }
}