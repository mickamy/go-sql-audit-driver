CREATE USER audriver_writer WITH PASSWORD 'password';
CREATE USER audriver_reader WITH PASSWORD 'password';

GRANT USAGE ON SCHEMA public TO
    audriver_writer,
    audriver_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO audriver_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO audriver_reader;

\echo '✅ Successfully created users.'
