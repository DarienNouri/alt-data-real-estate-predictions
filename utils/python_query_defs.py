

sql_data_daily_query = """
WITH sales_sub AS (
                      SELECT "SALE_DATE" AS date, COUNT(*) AS total_sales, AVG("SALE_PRICE") AS avg_price
                      FROM sales
                      GROUP BY "SALE_DATE"
                  ),
     complaints_sub AS (
                      SELECT date, COUNT(*) AS complaints
                      FROM complaints
                      GROUP BY date
                  ),
     businesses_sub AS (
                      SELECT date, COUNT(*) AS new_businesses
                      FROM businesses
                      GROUP BY date
                  ),
     evictions_sub AS (
                      SELECT date, COUNT(*) AS evictions
                      FROM evictions
                      GROUP BY date
                  ),
     restaurants_sub AS (
                      SELECT date, COUNT(*) AS new_restaurants
                      FROM restaurants
                      GROUP BY date
                  ),
     health_inspections_sub AS (
                      SELECT date, AVG("SCORE") AS avg_health_inspection, COUNT(*) AS total_inspections
                      FROM health_inspections
                      GROUP BY date
                  ),
     citibike_sub AS (
                      SELECT date, num_rides
                      FROM citibike_daily
                  ),
     arrests_sub AS (
                      SELECT "ARREST_DATE" AS date, COUNT(*) AS num_arrests
                      FROM nypd_arrests
                      GROUP BY "ARREST_DATE"
                  ),
     jobs_filed_sub AS (
                      SELECT "Pre- Filing Date" AS date, COUNT(*) AS jobs_filed
                      FROM job_application_filings
                      GROUP BY "Pre- Filing Date"
                  )
SELECT s.date :: date AS date
     , s.avg_price
     , s.total_sales
     , c.complaints
     , b.new_businesses
     , e.evictions
     , r.new_restaurants
     , h.avg_health_inspection
     , h.total_inspections
     , cb.num_rides   AS citibike_rides
     , a.num_arrests
     , j.jobs_filed
FROM sales_sub s
    LEFT JOIN complaints_sub c ON s.date = c.date
    LEFT JOIN businesses_sub b ON s.date = b.date
    LEFT JOIN evictions_sub e ON s.date = e.date
    LEFT JOIN restaurants_sub r ON s.date = r.date
    LEFT JOIN health_inspections_sub h ON s.date = h.date
    LEFT JOIN citibike_sub cb ON s.date = cb.date
    LEFT JOIN arrests_sub a ON s.date = a.date
    LEFT JOIN jobs_filed_sub j ON s.date = j.date
ORDER BY s.date
"""

sql_data_weekly_query = """
WITH
    sales_sub AS (
        SELECT TO_CHAR("SALE_DATE", 'IYYY-IW') AS yr_week
             , COUNT(*)                        AS total_sales
             , AVG("SALE_PRICE")               AS avg_price
        FROM sales
        GROUP BY yr_week
    )
  , complaints_sub AS (
        SELECT TO_CHAR(date, 'IYYY-IW') AS yr_week, COUNT(*) AS complaints
        FROM complaints
        GROUP BY yr_week
    )
  , businesses_sub AS (
        SELECT TO_CHAR(date, 'IYYY-IW') AS yr_week, COUNT(*) AS new_businesses
        FROM businesses
        GROUP BY yr_week
    )
  , evictions_sub AS (
        SELECT TO_CHAR(date, 'IYYY-IW') AS yr_week, COUNT(*) AS evictions
        FROM evictions
        GROUP BY yr_week
    )
  , restaurants_sub AS (
        SELECT TO_CHAR(date, 'IYYY-IW') AS yr_week, COUNT(*) AS new_restaurants
        FROM restaurants
        GROUP BY yr_week
    )
  , health_inspections_sub AS (
        SELECT TO_CHAR(date, 'IYYY-IW') AS yr_week
             , AVG("SCORE")             AS avg_health_inspection
             , COUNT(*)                 AS total_inspections
        FROM health_inspections
        GROUP BY yr_week
    )

SELECT s.yr_week
     , s.total_sales
     , s.avg_price
     , c.complaints
     , b.new_businesses
     , e.evictions
     , r.new_restaurants
     , h.avg_health_inspection
     , h.total_inspections
FROM sales_sub s
         LEFT JOIN complaints_sub c ON s.yr_week = c.yr_week
         LEFT JOIN businesses_sub b ON s.yr_week = b.yr_week
         LEFT JOIN evictions_sub e ON s.yr_week = e.yr_week
         LEFT JOIN restaurants_sub r ON s.yr_week = r.yr_week
         LEFT JOIN health_inspections_sub h ON s.yr_week = h.yr_week
ORDER BY s.yr_week;
"""