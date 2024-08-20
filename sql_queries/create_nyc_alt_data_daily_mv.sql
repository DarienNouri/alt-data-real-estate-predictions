DROP MATERIALIZED VIEW IF EXISTS nyc_alt_data_daily;
CREATE MATERIALIZED VIEW nyc_alt_data_daily AS 
WITH sales_sub AS (
  SELECT to_char("SALE_DATE", 'YYYY-MM-DD') AS event_date,
    COUNT(*) AS total_sales,
    AVG("SALE_PRICE") AS avg_price
  FROM sales
  GROUP BY event_date
),
complaints_sub AS (
  SELECT to_char(date, 'YYYY-MM-DD') AS event_date,
    COUNT(*) AS complaints
  FROM complaints
  GROUP BY event_date
),
businesses_sub AS (
  SELECT to_char(date, 'YYYY-MM-DD') AS event_date,
    COUNT(*) AS new_businesses
  FROM businesses
  GROUP BY event_date
),
evictions_sub AS (
  SELECT to_char(date, 'YYYY-MM-DD') AS event_date,
    COUNT(*) AS evictions
  FROM evictions
  GROUP BY event_date
),
restaurants_sub AS (
  SELECT format_date AS event_date,
    COUNT(*) AS new_restaurants
  FROM (
      SELECT *,
        to_char(date, 'YYYY-MM-DD') AS format_date
      FROM restaurants
    ) AS formatted_dates
  GROUP BY format_date
),
health_inspections_sub AS (
  SELECT to_char(date, 'YYYY-MM-DD') AS event_date,
    AVG("SCORE") AS avg_health_inspection,
    COUNT(*) AS total_inspections
  FROM health_inspections
  GROUP BY event_date
),
citibike_sub AS (
  SELECT to_char(date, 'YYYY-MM-DD') AS event_date,
    num_rides
  FROM citibike_daily
),
arrests_sub AS (
  SELECT to_char(g.arrest_date, 'YYYY-MM-DD') AS event_date,
    COUNT(g.*) AS num_arrests
  FROM (
      SELECT to_date("ARREST_DATE", 'MM/DD/YYYY') AS arrest_date
      FROM nypd_arrests
    ) AS g
  GROUP BY event_date
),
jobs_filed_sub AS (
  SELECT to_char(t.filing_date, 'YYYY-MM-DD') AS event_date,
    COUNT(t.*) AS jobs_filed
  FROM (
      SELECT to_date("Pre- Filing Date", 'MM/DD/YYYY') AS filing_date
      FROM job_application_filings
    ) AS t
  GROUP BY event_date
)
SELECT  TO_DATE(s.event_date,'YYYY-MM-DD') AS event_date
       ,s.avg_price
       ,s.total_sales
       ,c.complaints
       ,b.new_businesses
       ,e.evictions
       ,r.new_restaurants
       ,h.avg_health_inspection
       ,h.total_inspections
       ,a.num_arrests
       ,j.jobs_filed
       ,cb.num_rides AS citibike_rides
FROM sales_sub s
  LEFT JOIN complaints_sub c ON s.event_date = c.event_date
  LEFT JOIN businesses_sub b ON s.event_date = b.event_date
  LEFT JOIN evictions_sub e ON s.event_date = e.event_date
  LEFT JOIN restaurants_sub r ON s.event_date = r.event_date
  LEFT JOIN health_inspections_sub h ON s.event_date = h.event_date
  LEFT JOIN arrests_sub a ON s.event_date = a.event_date
  LEFT JOIN jobs_filed_sub j ON s.event_date = j.event_date
  LEFT JOIN citibike_sub cb ON s.event_date = cb.event_date
ORDER BY s.event_date;
SELECT *
FROM nyc_alt_data_daily as s
LIMIT 5;