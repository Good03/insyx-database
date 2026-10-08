-- POSTGRES_DB may already be one of these databases on first container startup.
SELECT format('CREATE DATABASE %I', name)
FROM unnest(ARRAY['nessie', 'scisci_postgres', 'scisci_lakehouse', 'insyx']) AS databases(name)
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = name)
\gexec

\connect insyx
\ir migrations/001_users.sql
\ir migrations/002_users_auth_compat.sql
