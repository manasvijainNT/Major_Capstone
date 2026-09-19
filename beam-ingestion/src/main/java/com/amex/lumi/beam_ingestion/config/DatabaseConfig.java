package com.amex.lumi.beam_ingestion.config;

public class DatabaseConfig {

    public static final String HOST =
            System.getenv("LUMI_DB_HOST");

    public static final int PORT =
            Integer.parseInt(System.getenv("LUMI_DB_PORT"));

    public static final String DATABASE =
            System.getenv("LUMI_DB_NAME");

    public static final String USERNAME =
            System.getenv("LUMI_DB_USER");

    public static final String PASSWORD =
            System.getenv("LUMI_DB_PASSWORD");

    public static String getJdbcUrl() {

        return "jdbc:postgresql://"
                + HOST
                + ":"
                + PORT
                + "/"
                + DATABASE;
    }
    public static String getUsername(){

        return USERNAME;
    }
    public static String getPassword(){

        return PASSWORD;
    }

}
