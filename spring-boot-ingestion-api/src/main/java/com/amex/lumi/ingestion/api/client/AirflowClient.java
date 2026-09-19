package com.amex.lumi.ingestion.api.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Map;

@Component
public class AirflowClient {

    private final RestClient restClient;
    private final String airflowUsername;
    private final String airflowPassword;

    private static final Logger log = LoggerFactory.getLogger(AirflowClient.class);

    public AirflowClient(
            @Value("${airflow.base-url}") String airflowBaseUrl,
            @Value("${airflow.username}") String airflowUsername,
            @Value("${airflow.password}") String airflowPassword
    ) {

        this.airflowUsername = airflowUsername;
        this.airflowPassword = airflowPassword;

        this.restClient = RestClient.builder()
                .baseUrl(airflowBaseUrl)
                .build();
    }

    public void triggerDag(
            String dagId,
            String fileLocation,
            String controlFileLocation,
            String executionId
    ) {

        Map<String, Object> requestBody =
                Map.of(
                        "conf",
                        Map.of(
                                "file_location", fileLocation,
                                "control_file_location", controlFileLocation,
                                "execution_id", executionId
                        )
                );

        restClient.post()
                .uri("/api/v1/dags/{dagId}/dagRuns", dagId)
                .contentType(MediaType.APPLICATION_JSON)
                .headers(headers ->
                        headers.setBasicAuth(
                                airflowUsername,
                                airflowPassword
                        )
                )
                .body(requestBody)
                .retrieve()
                .toBodilessEntity();
        log.info("Airflow DAG triggered successfully");
    }
    public void triggerDagWithChunks(
            String dagId,
            List<String> chunkLocations,
            String controlFileLocation,
            String executionId
    ) {
        Map<String, Object> requestBody =
                Map.of(
                        "conf",
                        Map.of(
                                "chunk_locations", chunkLocations,
                                "control_file_location",
                                controlFileLocation,
                                "execution_id",
                                executionId
                        )
                );
        restClient.post()
                .uri(
                        "/api/v1/dags/{dagId}/dagRuns",
                        dagId
                )
                .contentType(MediaType.APPLICATION_JSON)
                .headers(headers ->
                        headers.setBasicAuth(
                                airflowUsername,
                                airflowPassword
                        )
                )
                .body(requestBody)
                .retrieve()
                .toBodilessEntity();
        log.info(
                "Airflow DAG triggered successfully with {} chunks",
                chunkLocations.size()
        );
    }
}

