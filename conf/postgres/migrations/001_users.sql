-- Application accounts are separate from scientific authors and seed data.
BEGIN;
CREATE TABLE IF NOT EXISTS public.users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(254) NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    avatar_url VARCHAR(2048),
    google_subject VARCHAR(255),
    email_verified BOOLEAN NOT NULL DEFAULT FALSE,
    last_login_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT users_email_unique UNIQUE (email),
    CONSTRAINT users_google_subject_unique UNIQUE (google_subject),
    CONSTRAINT users_email_normalized CHECK (email = lower(btrim(email)) AND email <> ''),
    CONSTRAINT users_display_name_nonempty CHECK (btrim(display_name) <> ''),
    CONSTRAINT users_google_subject_nonempty CHECK (google_subject IS NULL OR btrim(google_subject) <> '')
);
COMMIT;
