-- =============================================================================
-- Shared SQL functions (repeatable). SQL UDFs only - no Python.
-- =============================================================================

-- Deterministic surrogate key for an SCD2 version (ADR-006). The SAME function is used by
-- the SCD2 rebuild and by inferred-member creation, so an inferred member (effective_from =
-- 1900-01-01) and the real first version always get the SAME key: facts that point at an
-- inferred product "fill in" automatically when the real product arrives.
CREATE OR REPLACE FUNCTION {{DATABASE}}.ANALYTICS.SCD2_KEY(NATURAL_KEY VARCHAR, EFFECTIVE_FROM TIMESTAMP_NTZ)
RETURNS NUMBER(38,0)
COMMENT = 'MD5_NUMBER_LOWER64(natural_key | effective_from) - reproducible across rebuilds'
AS
$$
    MD5_NUMBER_LOWER64(NATURAL_KEY || '|' || TO_VARCHAR(EFFECTIVE_FROM, 'YYYY-MM-DD HH24:MI:SS.FF9'))
$$;

-- First version of every SCD2 key starts at the beginning of time so any fact finds a version.
CREATE OR REPLACE FUNCTION {{DATABASE}}.ANALYTICS.BEGINNING_OF_TIME()
RETURNS TIMESTAMP_NTZ
AS
$$
    '1900-01-01 00:00:00'::TIMESTAMP_NTZ
$$;

CREATE OR REPLACE FUNCTION {{DATABASE}}.ANALYTICS.END_OF_TIME()
RETURNS TIMESTAMP_NTZ
AS
$$
    '9999-12-31 00:00:00'::TIMESTAMP_NTZ
$$;

CREATE OR REPLACE FUNCTION {{DATABASE}}.ANALYTICS.DATE_KEY(D DATE)
RETURNS NUMBER(8,0)
AS
$$
    TO_NUMBER(TO_CHAR(D, 'YYYYMMDD'))
$$;
