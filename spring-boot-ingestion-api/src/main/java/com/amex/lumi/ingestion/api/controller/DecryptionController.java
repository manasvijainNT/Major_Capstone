package com.amex.lumi.ingestion.api.controller;

import com.amex.lumi.ingestion.api.service.DecryptionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class DecryptionController {

    private final DecryptionService decryptionService;

    public DecryptionController(
            DecryptionService decryptionService) {

        this.decryptionService =
                decryptionService;
    }

    @GetMapping("/decrypt/{employeeId}")
    public ResponseEntity<Map<String, Object>>
    decryptEmployee(
            @PathVariable String employeeId){

        Map<String, Object> employee =
                decryptionService
                        .getDecryptedEmployee(
                                employeeId
                        );

        return ResponseEntity.ok(employee);
    }
}