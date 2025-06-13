CREATE TABLE database_modifications
(
    id           UUID                         NOT NULL PRIMARY KEY,
    operator_id  UUID                         NOT NULL,
    execution_id UUID                         NOT NULL,
    table_name   VARCHAR(63)                  NOT NULL,
    action       database_modification_action NOT NULL,
    sql          TEXT                         NOT NULL,
    modified_at  TIMESTAMPTZ                  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_database_modifications_execution_id ON database_modifications (execution_id);
CREATE INDEX idx_database_modifications_operator_id ON database_modifications (operator_id);
CREATE INDEX idx_database_modifications_table_name_action ON database_modifications (table_name, action);
