-- NYC alt_data mini EDA: Trends, Outliers, and Correlations

-- Take a look at rows w/ missing values
SELECT
     * FROM nyc_alt_data_daily
WHERE
    avg_price IS NULL
    OR new_restaurants IS NULL
    OR total_sales IS NULL
    OR complaints IS NULL
    OR new_businesses IS NULL
    OR evictions IS NULL
    OR avg_health_inspection IS NULL
    OR total_inspections IS NULL
    OR num_arrests IS NULL
    OR jobs_filed IS NULL
;

-- upsample data to monthly
SELECT
    to_char(date_trunc('month', u.event_date),'YYYY-MM-DD') AS month,
    AVG(u.avg_price::numeric) AS avg_sale,
    AVG(u.new_restaurants::numeric) AS avg_rest_app,
    AVG(u.total_sales::numeric) AS avg_prop_count,
    AVG(u.complaints::numeric) AS avg_311,
    AVG(u.new_businesses::numeric) AS avg_new_biz,
    AVG(u.evictions::numeric) AS avg_evict,
    AVG(u.avg_health_inspection::numeric) AS avg_rest_insp,
    AVG(u.total_inspections::numeric) AS avg_rest_insp_ct,
    AVG(u.num_arrests::numeric) AS avg_arrest,
    AVG(u.jobs_filed::numeric) AS avg_job_ct
FROM nyc_alt_data_daily u
GROUP BY 1
ORDER BY 1;


-- Top 10 months with the highest rate of change in avg_sale
WITH 
    monthly_avg AS (
        SELECT to_char(date_trunc('month', u.event_date), 'YYYY-MM') AS month,
            AVG(u.avg_price) AS avg_sale
        FROM nyc_alt_data_daily u
        GROUP BY 1
    ),
    avg_trended AS (
        SELECT  month
            ,avg_sale
            ,LAG(avg_sale) OVER ( ORDER BY  month ) AS prev_avg_sale
            ,AVG(avg_sale) OVER ( ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ) AS six_month_avg
            ,( avg_sale - LAG(avg_sale) OVER ( ORDER BY month ) ) / NULLIF( LAG(avg_sale) OVER ( ORDER BY month ),0 ) AS rate_of_change
            ,CASE WHEN avg_sale > AVG(avg_sale) OVER ( ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ) THEN 'increasing'  ELSE 'decreasing' END AS trend
        FROM monthly_avg
    ),
    ranked_data AS (
        SELECT  trend
            ,month
            ,rate_of_change
            ,avg_sale
            ,RANK() OVER ( PARTITION BY trend ORDER BY  rate_of_change DESC ) AS rank
        FROM avg_trended
    )
SELECT  trend
       ,month
       ,rate_of_change
       ,avg_sale
FROM ranked_data
WHERE rank <= 10
ORDER BY trend, rank;


-- Aggregate monthly real estate idx IN identifying relationships WITH other features during periods of strict up/down trends
WITH upsampled AS
(
    SELECT  to_char(date_trunc('month',u.event_date),'YYYY-MM-DD') AS month
           ,AVG(u.avg_price) AS avg_sale
           ,AVG(u.new_restaurants) AS avg_rest_app
           ,AVG(u.total_sales) AS avg_prop_count
           ,AVG(u.complaints) AS avg_311
           ,AVG(u.new_businesses) AS avg_new_biz
           ,AVG(u.evictions) AS avg_evict
           ,AVG(u.avg_health_inspection) AS avg_rest_insp
           ,AVG(u.total_inspections) AS avg_rest_insp_ct
           ,AVG(u.num_arrests) AS avg_arrest
           ,AVG(u.jobs_filed) AS avg_job_ct
    FROM nyc_alt_data_daily u
    GROUP BY  1
    ORDER BY  1
), avg_diff AS
(
    SELECT  u.*
           ,AVG(u.avg_sale) OVER w AS sma_sale
           ,(u.avg_sale - LAG(u.avg_sale) OVER w) / NULLIF(LAG(u.avg_sale) OVER w,0) AS diff_sale
           ,(u.avg_rest_app - LAG(u.avg_rest_app) OVER w) / NULLIF(LAG(u.avg_rest_app) OVER w,0) AS diff_rest_app
           ,(u.avg_prop_count - LAG(u.avg_prop_count) OVER w) / NULLIF(LAG(u.avg_prop_count) OVER w,0) AS diff_prop_ct
           ,(u.avg_311 - LAG(u.avg_311) OVER w) / NULLIF(LAG(u.avg_311) OVER w,0) AS diff_311
           ,(u.avg_new_biz - LAG(u.avg_new_biz) OVER w) / NULLIF(LAG(u.avg_new_biz) OVER w,0) AS diff_new_biz
           ,(u.avg_evict - LAG(u.avg_evict) OVER w) / NULLIF(LAG(u.avg_evict) OVER w,0) AS diff_evict
           ,(u.avg_rest_insp - LAG(u.avg_rest_insp) OVER w) / NULLIF(LAG(u.avg_rest_insp) OVER w,0) AS diff_rest_insp
           ,( u.avg_rest_insp_ct - LAG(u.avg_rest_insp_ct) OVER w ) / NULLIF(LAG(u.avg_rest_insp_ct) OVER w,0) AS diff_rest_insp_ct
           ,(u.avg_arrest - LAG(u.avg_arrest) OVER w) / NULLIF(LAG(u.avg_arrest) OVER w,0) AS diff_arrest
           ,(u.avg_job_ct - LAG(u.avg_job_ct) OVER w) / NULLIF(LAG(u.avg_job_ct) OVER w,0) AS diff_job_ct
    FROM upsampled u WINDOW w AS
    (
        ORDER BY u.month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )
), trended AS
(
    SELECT  a.*
           ,CASE WHEN a.avg_sale > a.sma_sale THEN 'increasing'  ELSE 'decreasing' END AS trend
    FROM avg_diff a
)
SELECT  t.trend
       ,AVG(t.diff_sale::numeric) AS avg_diff_sale
       ,AVG(t.diff_rest_app::numeric) AS avg_diff_rest_app
       ,AVG(t.diff_prop_ct::numeric) AS avg_diff_prop_ct
       ,AVG(t.diff_311::numeric) AS avg_diff_311
       ,AVG(t.diff_new_biz::numeric) AS avg_diff_new_biz
       ,AVG(t.diff_evict::numeric) AS avg_diff_evict
       ,AVG(t.diff_rest_insp::numeric) AS avg_diff_rest_insp
       ,AVG(t.diff_rest_insp_ct::numeric) AS avg_diff_rest_insp_ct
       ,AVG(t.diff_arrest::numeric) AS avg_diff_arrest
       ,AVG(t.diff_job_ct::numeric) AS avg_diff_job_ct
FROM trended t
GROUP BY  t.trend;


-- Detect outliers of yoy rest health insp USING z-score deviations
WITH upsampled AS
(
    SELECT  TO_CHAR( date_trunc('month',u.event_date) + INTERVAL '1 month' - INTERVAL '1 day','YYYY-MM-DD' ) AS month
           ,AVG(u.avg_health_inspection) AS avg_rest_insp
    FROM nyc_alt_data_daily u
    GROUP BY  1
    ORDER BY  1
), differenced AS
(
    SELECT  u.month
           ,u.avg_rest_insp AS level_health_insp
           ,u.avg_rest_insp - LAG(u.avg_rest_insp,12) OVER ( ORDER BY  u.month ) AS diff_rest_insp
    FROM upsampled u
), population AS
(
    SELECT  AVG(d.level_health_insp) AS avg_level_health_insp
           ,AVG(d.diff_rest_insp) AS mean_diff_rest_insp
           ,stddev(d.diff_rest_insp) AS std_diff_rest_insp
    FROM differenced d
), sliding_window AS
(
    SELECT  d.* -- SMA
           ,AVG(d.diff_rest_insp) OVER w AS sma_diff_rest_insp --Population avg AND STD
           ,AVG(d.diff_rest_insp) OVER () AS avg_pop_diff_rest_insp
           ,stddev(d.diff_rest_insp) OVER () AS pop_std_diff_rest_insp
           ,COUNT(d.diff_rest_insp) OVER () AS sample_size
    FROM differenced d WINDOW w AS
    (
        ORDER BY d.month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )
), deviators AS
(
    SELECT  a.*
           ,(a.sma_diff_rest_insp - avg_pop_diff_rest_insp) / NULLIF( (a.pop_std_diff_rest_insp / SQRT(a.sample_size)),0 )::numeric AS z_diff_rest_insp
    FROM sliding_window a
)
SELECT  d.month
       ,d.level_health_insp
       ,d.diff_rest_insp AS monthly_diff_health_insp
       ,d.sma_diff_rest_insp AS sma_sample_mean_diff_health_insp
       ,d.avg_pop_diff_rest_insp AS pop_mean_diff_health_insp
       ,d.pop_std_diff_rest_insp AS pop_std_diff_health_insp
       ,d.z_diff_rest_insp AS z_score_diff_health_insp_gt_absolute_1
FROM deviators d
WHERE ABS(d.z_diff_rest_insp) > 1.96
ORDER BY d.month;



-- fn to calculate Correlation TABLE of Upsampled YOY between all unique feature combinations
CREATE OR REPLACE FUNCTION calculate_corr(primary_col TEXT) RETURNS TABLE (col_name TEXT, correlation NUMERIC) AS $$ DECLARE col TEXT; query TEXT; BEGIN FOR col IN
SELECT  unnest(ARRAY['yoy_sale','yoy_rest_app','yoy_prop_count','yoy_311','yoy_new_biz','yoy_evict','yoy_rest_insp','yoy_rest_insp_ct','yoy_arrest','yoy_job_ct']) LOOP IF 1 THEN --col != primary_col THEN
 query := format($f$
            SELECT  CASE WHEN corr(yoy_diff.%I,yoy_diff.%I) != 1 THEN corr(yoy_diff.%I,yoy_diff.%I)::numeric  ELSE 1 END
            FROM
            ( -- 12 Month Differencin g
                SELECT  d.month
                    ,(d.avg_sale - LAG(d.avg_sale,12) OVER(ORDER BY  d.month)) AS yoy_sale
                    ,(d.avg_rest_app - LAG(d.avg_rest_app,12) OVER(ORDER BY d.month)) AS yoy_rest_app
                    ,(d.avg_prop_count - LAG(d.avg_prop_count,12) OVER(ORDER BY d.month)) AS yoy_prop_count
                    ,(d.avg_311 - LAG(d.avg_311,12) OVER(ORDER BY d.month)) AS yoy_311
                    ,(d.avg_new_biz - LAG(d.avg_new_biz,12) OVER(ORDER BY d.month)) AS yoy_new_biz
                    ,(d.avg_evict - LAG(d.avg_evict,12) OVER(ORDER BY d.month)) AS yoy_evict
                    ,(d.avg_rest_insp - LAG(d.avg_rest_insp,12) OVER(ORDER BY d.month)) AS yoy_rest_insp
                    ,(d.avg_rest_insp_ct - LAG(d.avg_rest_insp_ct,12) OVER(ORDER BY d.month)) AS yoy_rest_insp_ct
                    ,(d.avg_arrest - LAG(d.avg_arrest,12) OVER(ORDER BY d.month)) AS yoy_arrest
                    ,(d.avg_job_ct - LAG(d.avg_job_ct,12) OVER(ORDER BY d.month)) AS yoy_job_ct
                FROM
                ( -- Resampling Weekly -> Monthl y
                    SELECT  date_trunc('month',u.event_date) AS month
                        ,AVG(u.avg_price) AS avg_sale
                        ,AVG(u.new_restaurants) AS avg_rest_app
                        ,AVG(u.total_sales) AS avg_prop_count
                        ,AVG(u.complaints) AS avg_311
                        ,AVG(u.new_businesses) AS avg_new_biz
                        ,AVG(u.evictions) AS avg_evict
                        ,AVG(u.avg_health_inspection) AS avg_rest_insp
                        ,AVG(u.total_inspections) AS avg_rest_insp_ct
                        ,AVG(u.num_arrests) AS avg_arrest
                        ,AVG(u.jobs_filed) AS avg_job_ct
                    FROM nyc_alt_data_daily u
                    GROUP BY  1
                    ORDER BY  1 DESC
                ) d
            ) yoy_diff
                        WHERE yoy_diff.%I IS NOT NULL AND yoy_diff.%I IS NOT NULL
                        $f$, primary_col, col, primary_col, col, primary_col, col);
            EXECUTE query INTO correlation;
            col_name := col;
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Correlation Table of Upsampled YOY over all features
with corr_tbl as (
    SELECT 
        c1.col_name as feature,
        c1.correlation as yoy_sale,
        c8.correlation as yoy_rest_insp_ct,
        c7.correlation as yoy_rest_insp,
        c2.correlation as yoy_rest_app,
        c3.correlation as yoy_prop_count,
        c5.correlation as yoy_new_biz,
        c10.correlation as yoy_job_ct,
        c6.correlation as yoy_evict,
        c9.correlation as yoy_arrest,
        c4.correlation as yoy_311
    FROM
                  (SELECT t1.col_name, t1.correlation from calculate_corr('yoy_sale') t1) c1 
        LEFT JOIN (SELECT t2.col_name, t2.correlation from calculate_corr('yoy_rest_app') t2) c2 ON  c1.col_name = c2.col_name
        LEFT JOIN (SELECT t3.col_name, t3.correlation from calculate_corr('yoy_prop_count') t3) c3 ON  c1.col_name = c3.col_name
        LEFT JOIN (SELECT t4.col_name, t4.correlation from calculate_corr('yoy_311') t4) c4 ON  c1.col_name = c4.col_name
        LEFT JOIN (SELECT t5.col_name, t5.correlation from calculate_corr('yoy_new_biz') t5) c5 ON  c1.col_name = c5.col_name
        LEFT JOIN (SELECT t6.col_name, t6.correlation from calculate_corr('yoy_evict') t6) c6 ON  c1.col_name = c6.col_name
        LEFT JOIN (SELECT t7.col_name, t7.correlation from calculate_corr('yoy_rest_insp') t7) c7 ON  c1.col_name = c7.col_name
        LEFT JOIN (SELECT t8.col_name, t8.correlation from calculate_corr('yoy_rest_insp_ct') t8) c8 ON  c1.col_name = c8.col_name
        LEFT JOIN (SELECT t9.col_name, t9.correlation from calculate_corr('yoy_arrest') t9) c9 ON  c1.col_name = c9.col_name
        LEFT JOIN (SELECT t10.col_name, t10.correlation from calculate_corr('yoy_job_ct') t10)c10 ON c1.col_name = c10.col_name
    ORDER BY
        feature DESC
)
SELECT
* from corr_tbl;