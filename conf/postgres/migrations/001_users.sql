-- Application account schema; keep aligned with backend TypeORM migrations.
BEGIN;
CREATE TABLE IF NOT EXISTS public.users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(254) NOT NULL,
    name VARCHAR(200),
    affiliation VARCHAR(200),
    "passwordHash" VARCHAR,
    "googleId" VARCHAR(255),
    "resetTokenHash" VARCHAR,
    "resetTokenExpiresAt" TIMESTAMPTZ,
    "avatarUrl" VARCHAR(2048),
    "emailVerified" BOOLEAN NOT NULL DEFAULT FALSE,
    "lastLoginAt" TIMESTAMPTZ,
    "createdAt" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT users_email_unique UNIQUE (email),
    CONSTRAINT users_google_subject_unique UNIQUE ("googleId"),
    CONSTRAINT users_email_normalized CHECK (email = lower(btrim(email)) AND email <> ''),
    CONSTRAINT users_google_subject_nonempty CHECK ("googleId" IS NULL OR btrim("googleId") <> '')
);
COMMIT;
