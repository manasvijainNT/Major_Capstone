package com.amex.lumi.beam_ingestion.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class DatabaseInitializer {

    public static void initialize() throws Exception {

        String schemaPath =
                "/opt/airflow/database/schema.sql";


        String schemaSql =
                Files.readString(
                        Path.of(schemaPath)
                );

        try (
                Connection connection =
                        DriverManager.getConnection(
                                DatabaseConfig.getJdbcUrl(),
                                DatabaseConfig.getUsername(),
                                DatabaseConfig.getPassword()
                        );

                Statement statement =
                        connection.createStatement()
        ) {

            statement.execute(schemaSql);

        }
    }
}