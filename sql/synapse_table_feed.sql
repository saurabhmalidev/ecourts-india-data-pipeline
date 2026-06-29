-- dim_courts with SCD2 columns
CREATE TABLE dim_courts (
    court_key       INT IDENTITY(1,1) PRIMARY KEY,
    court_id        INT,
    court_name      VARCHAR(150),
    court_level     VARCHAR(20),
    parent_court_id INT,
    bench_name      VARCHAR(100),
    state_name      VARCHAR(100),
    judge_count     INT,
    valid_from      DATE,
    valid_to        DATE,
    is_current      BIT
);

-- fact_cases loaded from silver_cleaned
CREATE TABLE fact_cases (
    case_id             INT PRIMARY KEY,
    case_type           VARCHAR(10),
    case_subtype        VARCHAR(60),
    filing_date         DATE,
    status              VARCHAR(10),
    court_id            INT,
    last_modified       DATETIME,
    is_deleted          BIT,
    silver_transformed_at DATETIME
);