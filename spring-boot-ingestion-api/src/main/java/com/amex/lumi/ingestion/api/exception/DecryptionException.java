package com.amex.lumi.ingestion.api.exception;

public class DecryptionException extends RuntimeException {

    public DecryptionException(String message) {
        super(message);
    }
}