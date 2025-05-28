
alter session set use_cached_result = false;

SELECT
    --Query 01
    current_timestamp() ts,
    f.order_date,
    l.city,
    SUM(f.sales_total) AS total_sales,
    AVG(SUM(f.sales_total)) OVER (
        PARTITION BY l.city
        ORDER BY f.order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_avg
FROM 
contoso_sales_24b f
JOIN 
contoso_stores l
    ON f.store_id = l.store_id
GROUP BY
    f.order_date,
    l.city
ORDER BY
    l.city,
    f.order_date;




WITH monthly_sales AS (
  --Query 02
    SELECT
        current_timestamp() ts,
        DATE_TRUNC('month', f.order_date) AS sales_month,
        p.product_name,
        SUM(f.sales_total) AS total_sales
    FROM 
contoso_sales_24b f
 left join contoso_products p on f.product_id = p.product_id
    GROUP BY
        DATE_TRUNC('month', f.order_date),
        p.product_name
)
SELECT
current_timestamp() ts,
    sales_month,
    product_name,
    total_sales,
    RANK() OVER (PARTITION BY sales_month ORDER BY total_sales DESC) AS sales_rank
FROM monthly_sales
ORDER BY sales_month, sales_rank;





--Query 03
WITH season_sales AS (
  --Query 03
    SELECT
    current_timestamp() ts,
        l.city,
        l.state,
        CASE 
          WHEN MONTH(order_date) IN (12, 1, 2) THEN 'Winter'
          WHEN MONTH(order_date) IN (3, 4, 5) THEN 'Spring'
          WHEN MONTH(order_date) IN (6, 7, 8) THEN 'Summer'
          WHEN MONTH(order_date) IN (9, 10, 11) THEN 'Fall'
        END AS season,
        AVG(f.sales_total) AS avg_sales
    FROM 
contoso_sales_24b f
    JOIN 
contoso_stores l
        ON f.store_id = l.store_id
    GROUP BY
        l.city,
        l.state,
        season
)
SELECT
    current_timestamp() ts,
    city,
    state,
    season,
    avg_sales,
    sales_rank
FROM (
    SELECT
        city,
        state,
        season,
        avg_sales,
        DENSE_RANK() OVER (PARTITION BY season ORDER BY avg_sales DESC) AS sales_rank
    FROM season_sales
) t
WHERE sales_rank <= 3
ORDER BY season, sales_rank;






--Query 04
SELECT
--Query 04
    current_timestamp() ts,
    f.order_date,
    product_name,
    p.unit_price,
    p.unit_price * 0.7 as  standard_cost,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.sales_total) AS total_sales_total,
    (unit_price - standard_cost) * SUM(f.quantity) AS theoretical_margin
FROM 
contoso_sales_24b f
JOIN 
contoso_products p
    ON p.product_id = f.product_id
    AND f.order_date BETWEEN p.from_date AND p.to_date
GROUP BY
  ts,
    f.order_date,
    product_name,
    unit_price,
    standard_cost
ORDER BY
    f.order_date,
    product_name;


    


--Query 05
WITH daily_city_qty AS (
  --Query 05
    SELECT
        current_timestamp() ts,
        f.order_date,
        l.city,
        SUM(f.quantity) AS daily_qty
    FROM 
contoso_sales_24b f
    JOIN 
contoso_stores l
        ON f.store_id = l.store_id
    GROUP BY
        f.order_date,
        l.city
)
SELECT
    current_timestamp() ts,
    order_date,
    city,
    daily_qty,
    SUM(daily_qty) OVER (
        PARTITION BY city
        ORDER BY order_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30day_qty
FROM daily_city_qty
ORDER BY city, order_date;






--Query 06
CREATE OR REPLACE TABLE 
query06 AS
--Query 06
WITH monthly_cat AS (
    SELECT
        current_timestamp() ts,
        DATE_TRUNC('month', f.order_date) AS sales_month,
        p.category,
        SUM(f.sales_total) AS monthly_revenue
    FROM 
contoso_sales_24b f
    JOIN 
contoso_products p
        ON f.product_id = p.product_id
        AND f.order_date BETWEEN p.from_date AND p.to_date
    GROUP BY
        DATE_TRUNC('month', f.order_date),
        p.category
)
SELECT
    sales_month,
    category,
    monthly_revenue
FROM monthly_cat;







--Query 07
WITH yearly_sales AS (
  --Query 07
    SELECT
        current_timestamp() ts,
        l.store_id,
        l.city,
        l.state,
        YEAR(f.order_date) AS sales_year,
        SUM(f.sales_total) AS total_sales_year
    FROM 
contoso_sales_24b f
    JOIN 
contoso_stores l
        ON f.store_id = l.store_id 
    GROUP BY
        ts,
        l.store_id,
        l.city,
        l.state,
        YEAR(f.order_date)
)
SELECT
    ts,
    city,
    state,
    SUM(CASE WHEN sales_year = 2023 THEN total_sales_year ELSE 0 END) AS sales_2023,
    SUM(CASE WHEN sales_year = 2024 THEN total_sales_year ELSE 0 END) AS sales_2024,
    (SUM(CASE WHEN sales_year = 2024 THEN total_sales_year ELSE 0 END)
     - SUM(CASE WHEN sales_year = 2023 THEN total_sales_year ELSE 0 END)) AS yoy_diff
FROM yearly_sales
GROUP BY
   ts,
    city,
    state
ORDER BY
    city,
    state;





--Query 08
WITH city_quarter_subcat AS (
  --Query 08
    SELECT
    current_timestamp() ts,
        l.city,
        DATE_TRUNC('quarter', f.order_date) AS sales_quarter,
        p.category,
        SUM(f.sales_total) AS total_sales
    FROM 
contoso_sales_24b f
    JOIN 
contoso_stores l
        ON f.store_id = l.store_id
    JOIN 
contoso_products p
        ON f.product_id = p.product_id
        AND f.order_date BETWEEN p.from_date AND p.to_date
    GROUP BY
    ts,
        l.city,
        DATE_TRUNC('quarter', f.order_date),
        p.category
)
SELECT
ts,
    city,
    sales_quarter,
    category,
    total_sales,
    RANK() OVER (PARTITION BY city, sales_quarter ORDER BY total_sales DESC) AS subcat_rank
FROM city_quarter_subcat
ORDER BY city, sales_quarter, subcat_rank;







--Query 09
WITH daily_sales AS (
  --Query 09
    SELECT
      current_timestamp() ts,
        l.city,
        f.order_date,
        AVG(f.sales_total) AS avg_sales
    FROM 
contoso_sales_24b f
    JOIN 
contoso_stores l
        ON f.store_id = l.store_id
    GROUP BY
        l.city,
        f.order_date
)
SELECT 
    current_timestamp() ts,
    city,
    order_date,
    avg_sales,
    AVG(avg_sales) OVER (
        PARTITION BY city
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_avg_sales
FROM daily_sales
ORDER BY city, order_date;


--Query 10


CREATE OR REPLACE TABLE 
query10 AS
WITH daily_orders AS (
  --Query 10
    SELECT
        current_timestamp() ts,
        f.order_date,
        l.city,
        COUNT(DISTINCT f.store_id , f.product_id , f.order_date) AS daily_distinct_orders
    FROM 
contoso_sales_24b f
    JOIN 
contoso_stores l
        ON f.store_id = l.store_id
    GROUP BY
        f.order_date,
        l.city
)
SELECT 
    current_timestamp() ts,
    order_date,
    city,
    daily_distinct_orders,
    SUM(daily_distinct_orders) OVER (
        PARTITION BY city
        ORDER BY order_date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS rolling_90d_distinct_orders
FROM daily_orders
ORDER BY city, order_date;

--select count(*), min(daily_distinct_orders), max(rolling_90d_distinct_orders) from query10;




--Query 11
WITH city_quarter_subcat AS (
  --Query 11
    SELECT
    current_timestamp() ts,
        l.city,
        DATE_TRUNC('quarter', f.order_date) AS sales_quarter,
        p.category,
        SUM(f.sales_total) AS total_sales
    FROM 
contoso_sales_24b f
    JOIN 
contoso_stores l
        ON f.store_id = l.store_id
    JOIN 
contoso_products p
        ON f.product_id = p.product_id
        AND f.order_date BETWEEN p.from_date AND p.to_date
    WHERE l.city IN ('Charlotte', 'Houston')
    GROUP BY
        l.city,
        DATE_TRUNC('quarter', f.order_date),
        p.category
)
SELECT
current_timestamp() ts,
    city,
    sales_quarter,
    category,
    total_sales,
    RANK() OVER (PARTITION BY city, sales_quarter ORDER BY total_sales DESC) AS subcat_rank
FROM city_quarter_subcat
ORDER BY city, sales_quarter, subcat_rank;




--Query 12
WITH city_quarter_subcat AS (
  --Query 12
    SELECT
     current_timestamp() ts,
        l.city,
        DATE_TRUNC('quarter', f.order_date) AS sales_quarter,
        p.category,
        SUM(f.sales_total) AS total_sales
    FROM 
contoso_sales_24b f
    JOIN 
contoso_stores l
        ON f.store_id = l.store_id
    JOIN 
contoso_products p
        ON f.product_id = p.product_id
        AND f.order_date BETWEEN p.from_date AND p.to_date
    WHERE l.city IN ('Charlotte', 'Houston')
      AND DATE_TRUNC('quarter', f.order_date) IN (
            DATE('2023-01-01'), DATE('2023-04-01'),
            DATE('2024-01-01'), DATE('2024-04-01')
      )
    GROUP BY
        l.city,
        DATE_TRUNC('quarter', f.order_date),
        p.category
)
SELECT
current_timestamp() ts,
    city,
    sales_quarter,
    category,
    total_sales,
    RANK() OVER (PARTITION BY city, sales_quarter ORDER BY total_sales DESC) AS subcat_rank
FROM city_quarter_subcat
ORDER BY city, sales_quarter, subcat_rank;


--Query 13
WITH city_quarter_subcat AS (
  --Query 13
    SELECT
     current_timestamp() ts,
        l.city,
        DATE_TRUNC('quarter', f.order_date) AS sales_quarter,
        p.category,
        SUM(f.sales_total) AS total_sales
    FROM 
contoso_sales_24b f
    JOIN 
contoso_stores l
        ON f.store_id = l.store_id
    JOIN 
contoso_products p
        ON f.product_id = p.product_id
        AND f.order_date BETWEEN p.from_date AND p.to_date
    WHERE l.city = 'Austin'
      AND DATE_TRUNC('quarter', f.order_date) IN (
            DATE('2023-01-01'), DATE('2023-04-01'),
            DATE('2024-01-01'), DATE('2024-04-01')
      )
    GROUP BY
        l.city,
        DATE_TRUNC('quarter', f.order_date),
        p.category
)
SELECT
    city,
    sales_quarter,
    category,
    total_sales,
    RANK() OVER (PARTITION BY city, sales_quarter ORDER BY total_sales DESC) AS subcat_rank
FROM city_quarter_subcat
ORDER BY city, sales_quarter, subcat_rank;


--Query 14
WITH city_quarter_subcat AS (
  --Query 14
    SELECT
     current_timestamp() ts,
        l.city,
        DATE_TRUNC('quarter', f.order_date) AS sales_quarter,
        p.category,
        SUM(f.sales_total) AS total_sales
    FROM 
contoso_sales_24b f
    JOIN 
contoso_stores l
        ON f.store_id = l.store_id
    JOIN 
contoso_products p
        ON f.product_id = p.product_id
        AND f.order_date BETWEEN p.from_date AND p.to_date
    WHERE DATE_TRUNC('quarter', f.order_date) IN (
            DATE('2023-01-01'), DATE('2023-04-01'),
            DATE('2024-01-01'), DATE('2024-04-01')
      )
    GROUP BY
        l.city,
        DATE_TRUNC('quarter', f.order_date),
        p.category
)
SELECT
 current_timestamp() ts,
    city,
    sales_quarter,
    category,
    total_sales,
    RANK() OVER (PARTITION BY city, sales_quarter ORDER BY total_sales DESC) AS subcat_rank
FROM city_quarter_subcat
ORDER BY city, sales_quarter, subcat_rank;





--Query 15
CREATE OR REPLACE TABLE 
query15 AS
WITH base_data AS (
  --Query 15
    SELECT
        current_timestamp() ts,
        f.store_id,
        l.city,
        p.product_name,
        DATE_TRUNC('quarter', f.order_date) AS sales_quarter,
        SUM(f.sales_total) AS total_sales,
        SUM(f.sales_total * ( 5 / 100.0)) AS total_discount,
        SUM(f.quantity * (p.unit_price * 0.7)) AS total_cogs
    FROM 
contoso_sales_24b f
    JOIN 
contoso_products p
        ON f.product_id = p.product_id
        AND f.order_date BETWEEN p.from_date AND p.to_date
    JOIN 
contoso_stores l
        ON f.store_id = l.store_id
    WHERE f.order_date BETWEEN '2022-01-01' AND '2024-12-31'
    GROUP BY f.store_id, l.city, product_name, DATE_TRUNC('quarter', f.order_date)
),
with_profit AS (
    SELECT
        *,
        total_sales - total_discount - total_cogs AS profit
    FROM base_data
),
with_yoy AS (
    SELECT
        *,
        LAG(profit) OVER (PARTITION BY store_id, product_name ORDER BY sales_quarter) AS prev_profit,
        ROUND(
            CASE
                WHEN LAG(profit) OVER (PARTITION BY store_id, product_name ORDER BY sales_quarter) = 0 THEN NULL
                ELSE 100.0 * (profit - LAG(profit) OVER (PARTITION BY store_id, product_name ORDER BY sales_quarter)) /
                     LAG(profit) OVER (PARTITION BY store_id, product_name ORDER BY sales_quarter)
            END, 2
        ) AS yoy_profit_pct
    FROM with_profit
)
SELECT
    current_timestamp() ts,
    city,
    product_name,
    sales_quarter,
    profit,
    prev_profit,
    yoy_profit_pct
FROM with_yoy;







--Query 16
WITH seasonal_data AS (
  --Query 16
    SELECT
        current_timestamp() ts,
        l.state,
        CASE 
          WHEN MONTH(order_date) IN (12, 1, 2) THEN 'Winter'
          WHEN MONTH(order_date) IN (3, 4, 5) THEN 'Spring'
          WHEN MONTH(order_date) IN (6, 7, 8) THEN 'Summer'
          WHEN MONTH(order_date) IN (9, 10, 11) THEN 'Fall'
        END AS season,
        p.category,
        SUM(f.sales_total) AS total_sales,
        SUM(f.quantity) AS total_units,
        COUNT(DISTINCT  f.store_id || f.product_id || f.order_date) AS order_count
    FROM 
contoso_sales_24b f
    JOIN 
contoso_products p
        ON f.product_id = p.product_id
        AND f.order_date BETWEEN p.from_date AND p.to_date
    JOIN 
contoso_stores l
        ON f.store_id = l.store_id
    WHERE f.order_date BETWEEN '2023-01-01' AND '2024-06-30'
    GROUP BY l.state, season, p.category
),
ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY state, season ORDER BY total_sales DESC) AS category_rank
    FROM seasonal_data
)
SELECT *
FROM ranked
WHERE category_rank <= 3
ORDER BY state, season, category_rank;
