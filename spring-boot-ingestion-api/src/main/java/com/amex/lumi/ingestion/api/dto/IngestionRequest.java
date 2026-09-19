package com.amex.lumi.ingestion.api.dto;

import jakarta.validation.constraints.NotNull;
import org.springframework.web.multipart.MultipartFile;

public class IngestionRequest {

    private MultipartFile fileLocation;
    private MultipartFile controlFile;

    public MultipartFile getFileLocation() {
        return fileLocation;
    }

    public void setFileLocation(MultipartFile fileLocation) {
        this.fileLocation = fileLocation;
    }

    public MultipartFile getControlFile() {
        return controlFile;
    }

    public void setControlFile(MultipartFile controlFile) {
        this.controlFile = controlFile;
    }
}
