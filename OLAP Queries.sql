USE Metro;
-- Q1. Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
SELECT 
    p.product_name,
    MONTH(d.date) AS month,
    CASE
        WHEN d.is_weekend = TRUE THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    SUM(s.total_sale) AS total_revenue
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN dates d ON s.date_id = d.date_id
WHERE d.year = 2019  -- Specify the desired year here
GROUP BY p.product_name, MONTH(d.date), day_type
ORDER BY total_revenue DESC
LIMIT 5;

-- Q2. Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
WITH quarterly_revenue AS (
    SELECT 
        p.store_id,
        p.store_name,
        d.year,
        d.quarter,
        SUM(s.total_sale) AS total_revenue
    FROM 
        sales s
    JOIN 
        dates d ON s.date_id = d.date_id
    JOIN 
        products p ON s.product_id = p.product_id
    WHERE 
        d.year = 2019
    GROUP BY 
        p.store_id, p.store_name, d.year, d.quarter
),
revenue_with_lag AS (
    SELECT 
        qr.store_id,
        qr.store_name,
        qr.quarter,
        qr.total_revenue,
        LAG(qr.total_revenue) OVER (PARTITION BY qr.store_id ORDER BY qr.quarter) AS previous_quarter_revenue
    FROM 
        quarterly_revenue qr
)
SELECT 
    store_id,
    store_name,
    quarter,
    total_revenue,
    previous_quarter_revenue,
    CASE 
        WHEN previous_quarter_revenue IS NOT NULL THEN 
            ((total_revenue - previous_quarter_revenue) / previous_quarter_revenue) * 100
        ELSE 
            NULL
    END AS growth_rate
FROM 
    revenue_with_lag
ORDER BY 
    store_id, quarter;


-- Q3. Detailed Supplier Sales Contribution by Store and Product Name
SELECT 
    p.store_id,
    p.store_name,
    p.supplier_id,
    p.supplier_name,
    p.product_name,
    SUM(s.total_sale) AS total_sales_contribution
FROM 
    sales s
JOIN 
    products p ON s.product_id = p.product_id
GROUP BY 
    p.store_id, p.store_name, p.supplier_id, p.supplier_name, p.product_name
ORDER BY 
    p.store_id, 
    p.store_name, 
    p.supplier_name, 
    p.product_name;


-- Q4. Seasonal Analysis of Product Sales Using Dynamic Drill-Down
SELECT 
    p.product_name,
    CASE 
        WHEN d.month IN (3, 4, 5) THEN 'Spring'
        WHEN d.month IN (6, 7, 8) THEN 'Summer'
        WHEN d.month IN (9, 10, 11) THEN 'Fall'
        WHEN d.month IN (12, 1, 2) THEN 'Winter'
        ELSE 'Unknown'
    END AS season,
    SUM(s.total_sale) AS total_sales
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN dates d ON s.date_id = d.date_id
GROUP BY p.product_name, season
ORDER BY p.product_name, FIELD(season, 'Spring', 'Summer', 'Fall', 'Winter');


-- Q5. Store-Wise and Supplier-Wise Monthly Revenue Volatility
SELECT
    p.store_name, -- Store name from products table
    p.supplier_name, -- Supplier name from products table
    d.month, -- Month from the dates table
    SUM(s.total_sale) AS monthly_revenue, -- Total sales for the month
    LAG(SUM(s.total_sale), 1) OVER (PARTITION BY p.store_name, p.supplier_name ORDER BY d.month) AS previous_month_revenue,
    (SUM(s.total_sale) - LAG(SUM(s.total_sale), 1) OVER (PARTITION BY p.store_name, p.supplier_name ORDER BY d.month)) 
    / LAG(SUM(s.total_sale), 1) OVER (PARTITION BY p.store_name, p.supplier_name ORDER BY d.month) * 100 AS volatility
FROM sales s
JOIN products p ON s.product_id = p.product_id -- Join with products table to get store and supplier information
JOIN dates d ON s.date_id = d.date_id -- Join with dates table to get month and other date-related info
GROUP BY p.store_name, p.supplier_name, d.month -- Group by store, supplier, and month
ORDER BY p.store_name, p.supplier_name, d.month; -- Order by store, supplier, and month


-- Q6. Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis)
SELECT
    p1.product_name AS product_1,  -- First product name
    p2.product_name AS product_2,  -- Second product name
    COUNT(*) AS frequency  -- Frequency of the pair being purchased together
FROM sales s1
JOIN sales s2 ON s1.order_id = s2.order_id AND s1.product_id != s2.product_id -- Join on same order, different products
JOIN products p1 ON s1.product_id = p1.product_id  -- Join with products table to get product names for the first product
JOIN products p2 ON s2.product_id = p2.product_id  -- Join with products table to get product names for the second product
GROUP BY p1.product_name, p2.product_name  -- Group by product names
ORDER BY frequency DESC  -- Order by frequency of being purchased together, descending
LIMIT 5;  -- Get top 5 pairs of products

-- Q7. Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
SELECT
    p.store_name,  -- Store name is now part of the products table
    p.supplier_name,  -- Supplier name is part of the products table
    p.product_name,  -- Product name from the products table
    SUM(s.total_sale) AS total_sales  -- Total sales for the product in the store
FROM sales s
JOIN products p ON s.product_id = p.product_id  -- Join sales to products based on product_id
GROUP BY p.store_name, p.supplier_name, p.product_name WITH ROLLUP  -- Grouping by store, supplier, and product with ROLLUP for hierarchical summary
ORDER BY p.store_name, p.supplier_name, p.product_name;  -- Order by store, supplier, and product

-- Q8. Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
SELECT
    p.product_name,  -- Product name from the products table
    SUM(CASE WHEN MONTH(d.date) <= 6 THEN s.total_sale ELSE 0 END) AS H1_revenue,  -- H1 revenue (Jan - Jun)
    SUM(CASE WHEN MONTH(d.date) > 6 THEN s.total_sale ELSE 0 END) AS H2_revenue,  -- H2 revenue (Jul - Dec)
    SUM(CASE WHEN MONTH(d.date) <= 6 THEN s.quantity_ordered ELSE 0 END) AS H1_quantity,  -- H1 quantity ordered
    SUM(CASE WHEN MONTH(d.date) > 6 THEN s.quantity_ordered ELSE 0 END) AS H2_quantity,  -- H2 quantity ordered
    SUM(s.total_sale) AS total_revenue,  -- Total revenue for the product
    SUM(s.quantity_ordered) AS total_quantity  -- Total quantity ordered for the product
FROM sales s
JOIN products p ON s.product_id = p.product_id  -- Join sales table with products to get product details
JOIN dates d ON s.date_id = d.date_id  -- Join sales table with dates to get date-related details
GROUP BY p.product_name  -- Group by product name to get total sales and quantity per product
ORDER BY p.product_name;  -- Order by product name

-- Q9. Identify High Revenue Spikes in Product Sales and Highlight Outliers
WITH daily_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        d.date,
        SUM(s.total_sale) AS daily_sales
    FROM 
        sales s
    JOIN 
        products p ON s.product_id = p.product_id
    JOIN 
        dates d ON s.date_id = d.date_id
    GROUP BY 
        p.product_id, p.product_name, d.date
),
product_avg_sales AS (
    SELECT 
        product_id,
        product_name,
        AVG(daily_sales) AS daily_avg_sales
    FROM 
        daily_sales
    GROUP BY 
        product_id, product_name
),
spikes AS (
    SELECT 
        ds.product_id,
        ds.product_name,
        ds.date,
        ds.daily_sales,
        pas.daily_avg_sales,
        CASE 
            WHEN ds.daily_sales > 2 * pas.daily_avg_sales THEN 'Outlier'
            ELSE 'Normal'
        END AS status
    FROM 
        daily_sales ds
    JOIN 
        product_avg_sales pas ON ds.product_id = pas.product_id
)
SELECT 
    product_id,
    product_name,
    date,
    daily_sales,
    daily_avg_sales,
    status
FROM 
    spikes
ORDER BY 
    product_id, date;

-- Q9
WITH daily_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        d.date,
        SUM(s.total_sale) AS daily_sales
    FROM 
        sales s
    JOIN 
        products p ON s.product_id = p.product_id
    JOIN 
        dates d ON s.date_id = d.date_id
    GROUP BY 
        p.product_id, p.product_name, d.date
),
product_avg_sales AS (
    SELECT 
        product_id,
        product_name,
        AVG(daily_sales) AS daily_avg_sales
    FROM 
        daily_sales
    GROUP BY 
        product_id, product_name
),
spikes AS (
    SELECT 
        ds.product_id,
        ds.product_name,
        ds.date,
        ds.daily_sales,
        pas.daily_avg_sales,
        CASE 
            WHEN ds.daily_sales > 2 * pas.daily_avg_sales THEN 'Outlier'
            ELSE 'Normal'
        END AS status
    FROM 
        daily_sales ds
    JOIN 
        product_avg_sales pas ON ds.product_id = pas.product_id
)
SELECT 
    product_id,
    product_name,
    date,
    daily_sales,
    daily_avg_sales,
    status
FROM 
    spikes
ORDER BY 
    product_id, date;


-- Q10. Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT
    p.store_name,
    QUARTER(d.date) AS quarter,
    SUM(s.total_sale) AS total_sales
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN dates d ON s.date_id = d.date_id
GROUP BY p.store_name, QUARTER(d.date)
ORDER BY p.store_name, quarter;
SELECT * FROM STORE_QUARTERLY_SALES;


