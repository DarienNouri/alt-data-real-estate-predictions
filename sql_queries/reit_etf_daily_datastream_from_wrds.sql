-- Map entities to their CUSIP and other identifiers
WITH ticker_mapping AS (
    SELECT gvkey, tic, cusip, conm, cik, datadate
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY tic ORDER BY datadate DESC) AS rn
        FROM comp_na_daily_all.secd
        WHERE tic IN ('VNQ', 'MORT', 'REM', 'KBWY', 'NURE', 'RWR', 'ICF', 'SCHH', 'IYR', 'USRT', 'REET')
    ) AS ranked
    WHERE rn = 1
),
-- Map CUSIP to Datastream identifiers
datastream_mapping AS (
    SELECT tm.cusip AS cusip_tm, tm.conm, tm.gvkey, tm.tic AS ticker, datastream.*
    FROM tr_ds_equities.ds2cusipchg AS datastream
    JOIN ticker_mapping tm ON datastream.cusip = LEFT(tm.cusip, CHAR_LENGTH(tm.cusip) - 1)
    WHERE datastream.cusip IN (SELECT LEFT(cusip, CHAR_LENGTH(cusip) - 1) AS cusip FROM ticker_mapping)
),
-- Join Datastream daily stock data with the mapping tables
extensive_daily_data AS (
    SELECT ds_mapping.*, market_data.*
    FROM tr_ds_equities.wrds_ds2dsf AS market_data
    LEFT JOIN datastream_mapping ds_mapping ON market_data.infocode = ds_mapping.infocode
    WHERE market_data.infocode IN (SELECT infocode FROM datastream_mapping)
    AND market_data.date >= '2014-01-01'
    AND market_data.date <= '2023-03-01'
)

-- Final query to retrieve the desired data
SELECT *
FROM extensive_daily_data
WHERE marketdate >= '2014-01-01'
AND marketdate <= '2023-03-01';
