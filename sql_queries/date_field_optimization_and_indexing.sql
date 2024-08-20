-- Since we aggregate & join on date fields, creating 
-- indexes on these fields will speed up the queries.


-- check date field types across the tables
SELECT
    table_name,
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name IN ('sales', 'complaints', 'businesses', 'evictions', 'restaurants', 'health_inspections', 'citibike_daily', 'nypd_arrests', 'job_application_filings')
    AND column_name IN ('SALE_DATE', 'date', 'ARREST_DATE', 'Pre- Filing Date');


-- Loop through each tbl and date col to ensure they're type date
DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT
            table_name,
            column_name
        FROM
            information_schema.columns
        WHERE
            table_name IN ('sales', 'complaints', 'businesses', 'evictions', 'restaurants', 'health_inspections', 'citibike_daily', 'nypd_arrests', 'job_application_filings')
            AND column_name IN ('SALE_DATE', 'date', 'ARREST_DATE', 'Pre- Filing Date')
    LOOP
        EXECUTE format('
            ALTER TABLE %I
            ALTER COLUMN %I TYPE date USING %I::date;',
            rec.table_name,
            rec.column_name,
            rec.column_name);
    END LOOP;
END $$;

-- Create indexes on date columns
DO $$
DECLARE
    rec RECORD;
    idx_name TEXT;
BEGIN
    -- Loop through each table and date column to create indexes
    FOR rec IN
        SELECT
            table_name,
            column_name
        FROM
            information_schema.columns
        WHERE
            table_name IN ('sales', 'complaints', 'businesses', 'evictions', 'restaurants', 'health_inspections', 'citibike_daily', 'nypd_arrests', 'job_application_filings')
            AND column_name IN ('SALE_DATE', 'date', 'ARREST_DATE', 'Pre- Filing Date')
    LOOP
        -- Generate a consistent index name
        idx_name := 'idx_' || rec.table_name || '_' || rec.column_name;

        -- Drop the index if it exists and then create it
        EXECUTE format('
            DROP INDEX IF EXISTS %I;', idx_name);
        EXECUTE format('
            CREATE INDEX %I ON %I (%I);',
            idx_name,
            rec.table_name,
            rec.column_name);
    END LOOP;
END $$;

-- -- Convert date columns to date type using to_date function 
-- ALTER TABLE sales
-- ALTER COLUMN "SALE_DATE" TYPE date USING "SALE_DATE"::date;

-- ALTER TABLE complaints
-- ALTER COLUMN date TYPE date USING date::date;

-- ALTER TABLE businesses
-- ALTER COLUMN date TYPE date USING date::date;

-- ALTER TABLE evictions
-- ALTER COLUMN date TYPE date USING date::date;

-- ALTER TABLE restaurants
-- ALTER COLUMN date TYPE date USING date::date;

-- ALTER TABLE health_inspections
-- ALTER COLUMN date TYPE date USING date::date;

-- ALTER TABLE citibike_daily
-- ALTER COLUMN date TYPE date USING date::date;

-- ALTER TABLE nypd_arrests
-- ALTER COLUMN "ARREST_DATE" TYPE date USING "ARREST_DATE"::date;

-- ALTER TABLE job_application_filings
-- ALTER COLUMN "Pre- Filing Date" TYPE date USING "Pre- Filing Date"::date;



-- -- Create indexes on columns that already have appropriate date/time types
-- DROP INDEX IF EXISTS idx_sales_date;
-- CREATE INDEX idx_sales_date ON sales ("SALE_DATE");

-- DROP INDEX IF EXISTS idx_complaints_date;
-- CREATE INDEX idx_complaints_date ON complaints (date);

-- DROP INDEX IF EXISTS idx_businesses_date;
-- CREATE INDEX idx_businesses_date ON businesses (date);

-- DROP INDEX IF EXISTS idx_evictions_date;
-- CREATE INDEX idx_evictions_date ON evictions (date);

-- DROP INDEX IF EXISTS idx_restaurants_date;
-- CREATE INDEX idx_restaurants_date ON restaurants (date);

-- DROP INDEX IF EXISTS idx_health_inspections_date;
-- CREATE INDEX idx_health_inspections_date ON health_inspections (date);

-- DROP INDEX IF EXISTS idx_citibike_date;
-- CREATE INDEX idx_citibike_date ON citibike_daily (date);

-- DROP INDEX IF EXISTS idx_arrests_date;
-- CREATE INDEX idx_arrests_date ON nypd_arrests ("ARREST_DATE");


-- DROP INDEX IF EXISTS idx_jobs_filed_date;
-- CREATE INDEX idx_jobs_filed_date ON job_application_filings ("Pre- Filing Date");