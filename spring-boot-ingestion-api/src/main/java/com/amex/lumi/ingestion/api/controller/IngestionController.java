package com.amex.lumi.ingestion.api.controller;

import com.amex.lumi.ingestion.api.dto.IngestionRequest;
import com.amex.lumi.ingestion.api.dto.IngestionResponse;
import com.amex.lumi.ingestion.api.service.IngestionService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("api/v1/ingestions")
public class IngestionController {

    private final IngestionService ingestionService;

    public IngestionController(IngestionService ingestionService){
        this.ingestionService = ingestionService;
    }

    @PostMapping(consumes = "multipart/form-data")
    public ResponseEntity<IngestionResponse> startIngestion(
            @Valid @ModelAttribute IngestionRequest request){

        IngestionResponse response = ingestionService.startIngestion(request);

        return ResponseEntity.ok(response);
    }

}
