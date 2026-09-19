package com.amex.lumi.ingestion.api.dto;

public class IngestionResponse {

    private String executionId;

    private String message;

    public IngestionResponse(String executionId, String message){
        this.executionId = executionId;
        this.message = message;
    }

    public String getExecutionId() {
        return executionId;
    }

    public String getMessage(){
        return message;
    }
}
