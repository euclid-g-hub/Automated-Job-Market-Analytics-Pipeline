CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- fuzzy text search on job titles

DROP TABLE IF EXISTS job_skills   CASCADE;
DROP TABLE IF EXISTS jobs         CASCADE;
DROP TABLE IF EXISTS skills       CASCADE;
DROP TABLE IF EXISTS companies    CASCADE;
DROP TYPE  IF EXISTS employment_type_enum;
DROP TYPE  IF EXISTS seniority_enum;
DROP TYPE  IF EXISTS remote_enum;

CREATE TYPE employment_type_enum AS ENUM (
    'full_time',
    'part_time',
    'contract',
    'internship',
    'freelance',
    'unknown'
);

CREATE TYPE seniority_enum AS ENUM (
    'intern',
    'junior',
    'mid',
    'senior',
    'lead',
    'principal',
    'staff',
    'manager',
    'director',
    'vp',
    'c_level',
    'unknown'
);

CREATE TYPE remote_enum AS ENUM (
    'remote',
    'hybrid',
    'onsite',
    'unknown'
);

CREATE TABLE companies (
    company_id      SERIAL          PRIMARY KEY,

    name            VARCHAR(255)    NOT NULL,
    name_normalized VARCHAR(255)    NOT NULL,   -- lowercase, stripped for dedup

    website         VARCHAR(512),
    sector          VARCHAR(100),               
    size_range      VARCHAR(50),                
    hq_city         VARCHAR(100),
    hq_country      VARCHAR(100),
    founded_year    SMALLINT,

    first_seen_at   TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_company_name_normalized UNIQUE (name_normalized)
);

COMMENT ON TABLE  companies                IS 'One row per unique employer scraped across all sources.';
COMMENT ON COLUMN companies.name_normalized IS 'Lowercase, whitespace-collapsed version of name used for deduplication.';
COMMENT ON COLUMN companies.size_range      IS 'Employee count band, e.g. 11-50. Populated via enrichment step.';

CREATE TABLE skills (
    skill_id        SERIAL          PRIMARY KEY,
    name            VARCHAR(100)    NOT NULL,
    name_normalized VARCHAR(100)    NOT NULL,   
    category        VARCHAR(50),               

    CONSTRAINT uq_skill_name_normalized UNIQUE (name_normalized)
);

COMMENT ON TABLE  skills           IS 'Canonical skill taxonomy extracted from job listings.';
COMMENT ON COLUMN skills.category  IS 'Skill category: language, framework, cloud, database, ml, tool, other.';

INSERT INTO skills (name, name_normalized, category) VALUES
    -- Languages
    ('Python',          'python',           'language'),
    ('SQL',             'sql',              'language'),
    ('JavaScript',      'javascript',       'language'),
    ('TypeScript',      'typescript',       'language'),
    ('Java',            'java',             'language'),
    ('Scala',           'scala',            'language'),
    ('Go',              'go',               'language'),
    ('Rust',            'rust',             'language'),
    ('R',               'r',                'language'),
    ('C++',             'c++',              'language'),
    ('C#',              'c#',               'language'),
    -- Databases
    ('PostgreSQL',      'postgresql',       'database'),
    ('MySQL',           'mysql',            'database'),
    ('MongoDB',         'mongodb',          'database'),
    ('Redis',           'redis',            'database'),
    ('Elasticsearch',   'elasticsearch',    'database'),
    ('Snowflake',       'snowflake',        'database'),
    ('BigQuery',        'bigquery',         'database'),
    ('Cassandra',       'cassandra',        'database'),
    -- Cloud
    ('AWS',             'aws',              'cloud'),
    ('GCP',             'gcp',              'cloud'),
    ('Azure',           'azure',            'cloud'),
    ('Docker',          'docker',           'cloud'),
    ('Kubernetes',      'kubernetes',       'cloud'),
    ('Terraform',       'terraform',        'cloud'),
    -- ML / Data
    ('Pandas',          'pandas',           'ml'),
    ('scikit-learn',    'scikit-learn',     'ml'),
    ('PyTorch',         'pytorch',          'ml'),
    ('TensorFlow',      'tensorflow',       'ml'),
    ('Spark',           'spark',            'data'),
    ('Kafka',           'kafka',            'data'),
    ('Airflow',         'airflow',          'data'),
    ('dbt',             'dbt',              'data'),
    -- BI
    ('Power BI',        'power bi',         'bi'),
    ('Tableau',         'tableau',          'bi'),
    ('Looker',          'looker',           'bi')
ON CONFLICT (name_normalized) DO NOTHING;


CREATE TABLE jobs (
    job_id          SERIAL              PRIMARY KEY,
    company_id      INT                 NOT NULL
                        REFERENCES companies (company_id)
                        ON DELETE RESTRICT,

    source          VARCHAR(50)         NOT NULL,   -- 'hn_who_is_hiring', 'remoteok', etc.
    source_id       VARCHAR(255),                   -- original ID from the source
    url             VARCHAR(1024),

    -- Core fields
    title           VARCHAR(255),
    title_normalized VARCHAR(255),                  -- lowercase, for grouping
    employment_type employment_type_enum            NOT NULL DEFAULT 'unknown',
    seniority       seniority_enum                  NOT NULL DEFAULT 'unknown',
    remote_policy   remote_enum                     NOT NULL DEFAULT 'unknown',

    -- Location
    city            VARCHAR(100),
    country         VARCHAR(100),
    location_raw    VARCHAR(255),                   -- original string before parsing

    -- Salary
    salary_min      INT,                            -- annualised USD
    salary_max      INT,                            -- annualised USD
    salary_raw      VARCHAR(255),                   -- original string, audit trail
    salary_currency CHAR(3)             DEFAULT 'USD',

    -- Content
    description     TEXT,                           -- full listing text

    -- Dates
    posted_at       TIMESTAMPTZ,                    -- when the listing was published
    scraped_at      TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ,

    -- Quality flag — set to FALSE after deduplication / manual review
    is_active       BOOLEAN             NOT NULL DEFAULT TRUE,

    -- Dedup hash — SHA-256 of (source + source_id + title + company)
    content_hash    CHAR(64),

    CONSTRAINT uq_source_source_id  UNIQUE (source, source_id),
    CONSTRAINT chk_salary_order     CHECK  (
        salary_min IS NULL OR salary_max IS NULL OR salary_min <= salary_max
    ),
    CONSTRAINT chk_salary_positive  CHECK  (
        (salary_min IS NULL OR salary_min > 0) AND
        (salary_max IS NULL OR salary_max > 0)
    )
);

COMMENT ON TABLE  jobs                  IS 'One row per unique job posting across all scraped sources.';
COMMENT ON COLUMN jobs.source_id        IS 'Original identifier from the scrape source (e.g. HN comment ID).';
COMMENT ON COLUMN jobs.content_hash     IS 'SHA-256 of key fields; used to detect duplicate listings across sources.';
COMMENT ON COLUMN jobs.salary_min       IS 'Lower salary bound, annualised USD. NULL if not stated.';
COMMENT ON COLUMN jobs.salary_max       IS 'Upper salary bound, annualised USD. NULL if not stated.';
COMMENT ON COLUMN jobs.is_active        IS 'FALSE after expiry, deduplication, or manual removal.';

CREATE TABLE job_skills (
    job_id          INT     NOT NULL REFERENCES jobs   (job_id)  ON DELETE CASCADE,
    skill_id        INT     NOT NULL REFERENCES skills (skill_id) ON DELETE CASCADE,
    inferred        BOOLEAN NOT NULL DEFAULT FALSE,   -- TRUE if added by ML, not regex

    PRIMARY KEY (job_id, skill_id)
);

COMMENT ON TABLE  job_skills          IS 'Bridge table linking jobs to their required skills (many-to-many).';
COMMENT ON COLUMN job_skills.inferred IS 'TRUE when the skill was inferred by a classifier rather than explicit regex match.';


-- Most common filter in dashboard queries
CREATE INDEX idx_jobs_posted_at
    ON jobs (posted_at DESC);

-- Salary explorer slicer
CREATE INDEX idx_jobs_salary_min
    ON jobs (salary_min)
    WHERE salary_min IS NOT NULL;

-- Location-based filtering
CREATE INDEX idx_jobs_country_city
    ON jobs (country, city);

-- Remote policy filter (low-cardinality, partial index per value)
CREATE INDEX idx_jobs_remote_policy
    ON jobs (remote_policy);

-- Seniority filter
CREATE INDEX idx_jobs_seniority
    ON jobs (seniority);

-- Active listings only (used in almost every query)
CREATE INDEX idx_jobs_is_active
    ON jobs (is_active)
    WHERE is_active = TRUE;

-- Company lookup from jobs
CREATE INDEX idx_jobs_company_id
    ON jobs (company_id);

-- Fuzzy title search (requires pg_trgm)
CREATE INDEX idx_jobs_title_trgm
    ON jobs USING gin (title_normalized gin_trgm_ops);

-- Composite: the flagship analytical query
--   "average salary by skill, filtered by country, ordered by posted_at"
CREATE INDEX idx_jobs_analytics
    ON jobs (country, posted_at DESC, salary_min, salary_max)
    WHERE is_active = TRUE AND salary_min IS NOT NULL;

-- ── job_skills
-- Reverse lookup: all jobs requiring a given skill
CREATE INDEX idx_job_skills_skill_id
    ON job_skills (skill_id);

-- ── companies 

CREATE INDEX idx_companies_name_trgm
    ON companies USING gin (name_normalized gin_trgm_ops);

CREATE INDEX idx_companies_sector
    ON companies (sector);

-- ── skills

CREATE INDEX idx_skills_category
    ON skills (category);

-- ── 1. Salary by skill ─
-- "Which skills command the highest median salary?"

CREATE OR REPLACE VIEW vw_salary_by_skill AS
SELECT
    s.name                                      AS skill,
    s.category                                  AS skill_category,
    COUNT(DISTINCT js.job_id)                   AS job_count,
    PERCENTILE_CONT(0.5) WITHIN GROUP
        (ORDER BY j.salary_min)                 AS median_salary_min,
    PERCENTILE_CONT(0.5) WITHIN GROUP
        (ORDER BY j.salary_max)                 AS median_salary_max,
    AVG(j.salary_min)                           AS avg_salary_min,
    AVG(j.salary_max)                           AS avg_salary_max
FROM   skills       s
JOIN   job_skills   js ON js.skill_id   = s.skill_id
JOIN   jobs         j  ON j.job_id      = js.job_id
WHERE  j.is_active = TRUE
AND    j.salary_min IS NOT NULL
GROUP  BY s.skill_id, s.name, s.category
HAVING COUNT(DISTINCT js.job_id) >= 10   -- suppress skills with tiny samples
ORDER  BY median_salary_min DESC;

COMMENT ON VIEW vw_salary_by_skill IS
    'Median and average salary per skill. Minimum 10 listings required for inclusion.';


-- ── 2. Salary by skill + city (stored procedure)
-- "What does Python pay in Manila vs Singapore vs Remote?"

CREATE OR REPLACE FUNCTION fn_salary_by_skill_city(
    p_skill     VARCHAR DEFAULT NULL,    -- NULL = all skills
    p_country   VARCHAR DEFAULT NULL,    -- NULL = all countries
    p_min_jobs  INT     DEFAULT 5
)
RETURNS TABLE (
    skill               VARCHAR,
    city                VARCHAR,
    country             VARCHAR,
    job_count           BIGINT,
    median_salary_min   DOUBLE PRECISION,
    median_salary_max   DOUBLE PRECISION,
    avg_salary_min      DOUBLE PRECISION,
    avg_salary_max      DOUBLE PRECISION
)
LANGUAGE sql STABLE AS $$
    SELECT
        s.name,
        j.city,
        j.country,
        COUNT(DISTINCT j.job_id),
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY j.salary_min),
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY j.salary_max),
        AVG(j.salary_min),
        AVG(j.salary_max)
    FROM   skills       s
    JOIN   job_skills   js ON js.skill_id = s.skill_id
    JOIN   jobs         j  ON j.job_id    = js.job_id
    WHERE  j.is_active    = TRUE
    AND    j.salary_min   IS NOT NULL
    AND    (p_skill   IS NULL OR LOWER(s.name) = LOWER(p_skill))
    AND    (p_country IS NULL OR LOWER(j.country) = LOWER(p_country))
    GROUP  BY s.name, j.city, j.country
    HAVING COUNT(DISTINCT j.job_id) >= p_min_jobs
    ORDER  BY median_salary_min DESC;
$$;

COMMENT ON FUNCTION fn_salary_by_skill_city IS
    'Returns salary stats per skill+city combination. Pass NULL to any param to see all values.';


-- ── 3. Skill demand over time 
-- "Is demand for dbt rising or falling week-over-week?"

CREATE OR REPLACE VIEW vw_skill_demand_weekly AS
SELECT
    DATE_TRUNC('week', j.posted_at)             AS week_start,
    s.name                                      AS skill,
    s.category                                  AS skill_category,
    COUNT(DISTINCT js.job_id)                   AS listing_count
FROM   skills       s
JOIN   job_skills   js ON js.skill_id = s.skill_id
JOIN   jobs         j  ON j.job_id    = js.job_id
WHERE  j.is_active  = TRUE
AND    j.posted_at  IS NOT NULL
GROUP  BY DATE_TRUNC('week', j.posted_at), s.skill_id, s.name, s.category
ORDER  BY week_start DESC, listing_count DESC;

COMMENT ON VIEW vw_skill_demand_weekly IS
    'Weekly listing count per skill. Use in Power BI line chart to show rising/falling demand.';


-- ── 4. Remote-friendliness by city 
-- "Which cities genuinely post remote roles vs which just claim to?"

CREATE OR REPLACE VIEW vw_remote_by_city AS
SELECT
    j.city,
    j.country,
    COUNT(*)                                        AS total_listings,
    COUNT(*) FILTER (WHERE j.remote_policy = 'remote')  AS remote_count,
    COUNT(*) FILTER (WHERE j.remote_policy = 'hybrid')  AS hybrid_count,
    COUNT(*) FILTER (WHERE j.remote_policy = 'onsite')  AS onsite_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE j.remote_policy = 'remote')
        / NULLIF(COUNT(*), 0), 1
    )                                               AS remote_pct
FROM   jobs j
WHERE  j.is_active = TRUE
AND    j.city IS NOT NULL
GROUP  BY j.city, j.country
HAVING COUNT(*) >= 20
ORDER  BY remote_pct DESC;

COMMENT ON VIEW vw_remote_by_city IS
    'Remote-friendliness score per city. Minimum 20 listings required.';


-- ── 5. Executive summary 
-- Single-row snapshot for the Power BI header cards.

CREATE OR REPLACE VIEW vw_summary AS
SELECT
    COUNT(*)                                AS total_listings,
    COUNT(*) FILTER (WHERE is_active)       AS active_listings,
    COUNT(DISTINCT company_id)              AS total_companies,
    COUNT(*) FILTER
        (WHERE salary_min IS NOT NULL)      AS listings_with_salary,
    ROUND(AVG(salary_min))                  AS avg_salary_min,
    ROUND(AVG(salary_max))                  AS avg_salary_max,
    COUNT(*) FILTER
        (WHERE remote_policy = 'remote')    AS remote_listings,
    MIN(posted_at)                          AS earliest_listing,
    MAX(posted_at)                          AS latest_listing,
    MAX(scraped_at)                         AS last_scrape
FROM   jobs;

COMMENT ON VIEW vw_summary IS
    'Single-row snapshot of key metrics. Used for Power BI executive summary header cards.';