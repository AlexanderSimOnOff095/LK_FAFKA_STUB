CREATE TABLE IF NOT EXISTS application_status_transition (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    application_id VARCHAR(64) NOT NULL DEFAULT '*',
    application_type VARCHAR(50) NOT NULL DEFAULT 'DEFAULT',
    incoming_status VARCHAR(50) NOT NULL,
    target_status VARCHAR(50) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_application_status_transition UNIQUE (application_type, incoming_status, target_status)
);

CREATE TABLE IF NOT EXISTS service_settings (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    processing_mode VARCHAR(40) NOT NULL DEFAULT 'STATUS_TRANSITION',
    result_publish_delay_ms INTEGER NOT NULL DEFAULT 0 CHECK (result_publish_delay_ms BETWEEN 0 AND 30000),
    error_simulation_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    error_code VARCHAR(100) NOT NULL DEFAULT 'INTERNAL_ERROR',
    error_message VARCHAR(500) NOT NULL DEFAULT 'Test processing error',
    retryable BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_request (
    request_id VARCHAR(100) PRIMARY KEY,
    request_fingerprint CHAR(64) NOT NULL,
    result_payload JSONB NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO service_settings(id) VALUES (1) ON CONFLICT (id) DO NOTHING;
INSERT INTO application_status_transition(application_id,application_type,incoming_status,target_status,priority) VALUES
('*','DEFAULT','NEW','PROCESSING',1),('*','DEFAULT','PROCESSING','COMPLETED',1),
('*','DEFAULT','COMPLETED','COMPLETED',1),('*','DEFAULT','ERROR','PROCESSING',1)
ON CONFLICT DO NOTHING;
