CREATE TABLE IF NOT EXISTS employee (

    employee_id VARCHAR(100),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    phone_number TEXT,
    hire_date DATE,
    department VARCHAR(100),
    job_title VARCHAR(150),
    salary TEXT,
    currency VARCHAR(10),
    employment_status VARCHAR(50),
    manager_id VARCHAR(100),
    is_active BOOLEAN,
    skills TEXT,

    address_street VARCHAR(255),
    address_city VARCHAR(100),
    address_state VARCHAR(100),
    address_postal_code VARCHAR(20),
    address_country VARCHAR(100),

    emergency_name VARCHAR(100),
    emergency_relationship VARCHAR(50),
    emergency_phone TEXT,
    emergency_email VARCHAR(200),

    ingestion_timestamp TIMESTAMP,
    execution_id VARCHAR(100),
    source_creation_time TIMESTAMP
    );