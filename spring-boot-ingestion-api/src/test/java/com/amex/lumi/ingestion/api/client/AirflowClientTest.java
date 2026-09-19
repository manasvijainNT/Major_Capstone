package com.amex.lumi.ingestion.api.client;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;

class AirflowClientTest {

    @Test
    void testAirflowClientCreation() {

        AirflowClient client =
                new AirflowClient(
                        "http://localhost:8080",
                        "admin",
                        "admin"
                );

        assertNotNull(client);

        Object username =
                ReflectionTestUtils.getField(
                        client,
                        "airflowUsername"
                );

        Object password =
                ReflectionTestUtils.getField(
                        client,
                        "airflowPassword"
                );

        assertEquals("admin", username);
        assertEquals("admin", password);
    }

}