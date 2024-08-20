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