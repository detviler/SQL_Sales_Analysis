-- Creating table sales

CREATE TABLE sales(
	event_time TIMESTAMP with TIME ZONE,
    order_id BIGINT,
    product_id BIGINT,
    category_id BIGINT,
    category_code CHARACTER VARYING(100),
    brand CHARACTER VARYING(100),
    price MONEY,
    user_id BIGINT
);

-- Loading Data from Flat File

COPY sales (event_time, order_id, product_id, category_id, category_code, brand, price, user_id) 
FROM 'D:\kz.csv\kz.csv'
DELIMITER ',' 
CSV HEADER 
ENCODING 'UTF8';

-- Creating Index to improve read operations' performance

CREATE INDEX idx_price ON sales (price);
CREATE INDEX idx_product_category ON sales (product_id, category_id);
CREATE INDEX idx_category_id ON sales (category_id);

-- Checking for Blank Spaces

SELECT * FROM sales WHERE TRIM(CATEGORY_CODE)='';
SELECT * FROM sales WHERE TRIM(brand)='';
SELECT * FROM sales WHERE Price='';

-- Updating Missing Values/Blank Spaces to NULL values

UPDATE sales
SET category_code = NULL
WHERE TRIM(category_code)=''; --743069 Rows effected

UPDATE sales
SET brand = NULL
WHERE TRIM(brand)=''; --20664 Rows effected

UPDATE sales
SET price = NULL
WHERE price=''; --50 Rows effected

--Subquery to assigning category_codes to rows with "NULL values" based on information from product_id column. This is useful as if there is similar product_id where there is already a value populated for category_code that value can be populated for missing category_codes for other tables. 

UPDATE sales
SET category_code = (
    SELECT category_code
    FROM sales
    WHERE product_id = sales.product_id
      AND category_code IS NOT NULL
    LIMIT 1
)
WHERE category_code IS NULL;

-- Input Missing values for Price based on Average price of that Category

UPDATE sales AS s
SET price = COALESCE(s.price, t1.price::MONEY)
FROM (
	SELECT ROUND(AVG(price::NUMERIC)) AS price, category_id 
	FROM sales
	GROUP BY category_id
) t1
WHERE s.category_id = t1.category_id AND s.price IS NULL; --50 Rows effected

-- Exporting the Cleaned Dataset

COPY sales TO 'D:/cleaned_data.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');

-- Calculating Correlation Coefficient between Price and Total Revenue

WITH t1 AS (
	SELECT product_id, ROUND(SUM(price::NUMERIC), 2) AS total_revenue
	FROM sales
	GROUP BY product_id
)
SELECT CORR(s.price::NUMERIC, t1.total_revenue) AS correlation
FROM sales s
JOIN t1 ON s.product_id = t1.product_id;
	
SELECT CORR(price::NUMERIC, total_revenue) AS correlation_coefficient
FROM (
	SELECT product_id, SUM(price::NUMERIC) AS total_revenue
    FROM sales
    GROUP BY product_id
) AS t1
JOIN sales s ON t1.product_id = s.product_id;

------- Data Analysis ---------------------------------------
-- Calculate total revenue with category names

SELECT category_id, category_code, SUM(price::NUMERIC) AS total_revenue
FROM sales
GROUP BY category_id, category_code
ORDER BY total_revenue DESC;

---------------------------------
-- Top 5 Sales Categories

SELECT category_code, COUNT(*) AS sales_count
FROM sales
GROUP BY category_code
ORDER BY sales_count DESC
LIMIT 5;

---This query finds the top 5 user Ids which made the maximum purchases, used group by and order by function to get the results

COPY(SELECT user_id, SUM(price) AS total_purchases
FROM sales
WHERE user_id IS NOT NULL
GROUP BY user_id
ORDER BY total_purchases DESC
LIMIT 5) TO 'D:/Top5_users.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
  --output contains 5 rows with total purchases assorted in desc mode 

---This query runs to find the total revenue contribution of each brands. Trim will remove the leading and trailing spaces. Colaesce Replaces NULL values with 'Unknown' function in grouping. This view is great as seller can understand which brands are most sold under their account. 
COPY (SELECT COALESCE(NULLIF(TRIM(brand), ''), 'Unknown') AS brand, SUM(price) AS total_revenue
FROM sales
GROUP BY COALESCE(NULLIF(TRIM(brand), ''), 'Unknown')
ORDER BY total_revenue DESC
LIMIT 10) TO 'D:/Top10_Brands.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');


-- This query finds the average selling price for each brands, rounded to two decimal places. 

SELECT brand, ROUND(AVG(price::numeric), 2) AS average_selling_price
FROM sales
WHERE brand IS NOT NULL
GROUP BY brand
ORDER BY average_selling_price DESC; 

-- This query finds the total sales and average selling price for each day. The date data is extracted from event_time column. Good way to know which are the days sales are higher. 

SELECT DATE(event_time) AS sale_date,
COUNT(*) AS total_sales,
ROUND(AVG(price::numeric)) AS average_selling_price
FROM sales
GROUP BY sale_date
ORDER BY sale_date; 

--This query identifies which products are the top sellers for the month of 'April', query is retrieved based on individual order counts. 

COPY (SELECT
	  CONCAT_WS(' ',brand,SPLIT_PART(category_code, '.', 2),SPLIT_PART(category_code, '.', 3)) AS product,
	  COUNT(*) AS total_orders
	  FROM sales
	  WHERE EXTRACT(MONTH FROM event_time) = 4 AND brand IS NOT NULL
	  GROUP BY category_code, brand
	  ORDER BY total_orders DESC
	  LIMIT 5)
	  TO 'D:/Top10_Prod_Orders.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');

-- This query identifies the user who made the highest purchase on a certain date, in this instance on "Victoria Day - 22 May 2020"

SELECT user_id, SUM(price) AS total_purchase
FROM sales
WHERE DATE(event_time) = '2020-05-22' -- Replace with the desired date
GROUP BY user_id
ORDER BY total_purchase DESC
LIMIT 1;

--This query uses the RANK window function to assign a rank to each product based on its price within each category. This is rather usueful as it displays which products in the brand is priced in comparison to other products within the same brand.

SELECT DISTINCT product_id, brand, price,
RANK() OVER (PARTITION BY brand ORDER BY price DESC) AS price_rank
FROM sales
WHERE brand IS NOT NULL
ORDER BY brand, price DESC;

--This query extracts information from category_code, which stores data in a hierarchical format separated by dots. Examples of entries in this column include "furniture.kitchen.table" and "electronics.audio.headphone." Since the data is huge just to showcase limited it to 100

SELECT category_code,
SPLIT_PART(category_code, '.', 1) AS main_category,
SPLIT_PART(category_code, '.', 2) AS sub_category,
SPLIT_PART(category_code, '.', 3) AS sub_sub_category
FROM sales
LIMIT 100;

-- This query find users who have made multiple purchases, usueful query as seller can understand who are regular customers and can devise promptional plans for repeate customers. 

SELECT user_id, COUNT(*) AS purchase_count
FROM sales
WHERE user_id IS NOT NULL 
GROUP BY user_id
HAVING COUNT(*) > 1
ORDER BY purchase_count DESC; ---20895 rows impacted

--This query Analyze the distribution of product prices in different price ranges. Gives a fair idea the price range of products in our inventory. 

SELECT
CASE WHEN price::numeric < 50 THEN '0-50'
     WHEN price::numeric < 100 THEN '50-100'
     WHEN price::numeric < 200 THEN '100-200'
	 ELSE '200+'
     END AS price_range,
COUNT(*) AS product_count
FROM sales
GROUP BY price_range; -- 4 rows impacted

--- This query firstly identifies the average order value and then the counts of orders for user IDs, this was we can identify who are the buyers who purchased more than once and having a significantly higher average order value.

SELECT user_id,
COUNT(*) AS order_count,
ROUND(AVG(price::numeric)) AS avg_order_value
FROM sales
WHERE user_id IS NOT NULL
GROUP BY user_id
ORDER BY avg_order_value DESC;


-- Gives the monthly sales trend. Good for visualization and analyse the growth or decline in sales as a function of time 

COPY(SELECT
	 to_char(event_time, 'Month') AS month_name,
     SUM(price) AS monthly_revenue
	 FROM sales
	 GROUP BY month_name
	 ORDER BY month_name) TO 'D:/Monthly_Sales_Trend.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ','); 

---Using case function to label users based on their purchase count. 

WITH t1 AS (
	SELECT user_id,
	CASE WHEN COUNT(*) = 1 THEN 'Single Purchase'
         WHEN COUNT(*) = 2 THEN 'Occasional Buyer'
         WHEN COUNT(*) >= 3 THEN 'Frequent Buyer'
    ELSE 'No Purchases' END AS user_category
	FROM sales
	WHERE user_id IS NOT NULL 
	GROUP BY user_id
)
SELECT COUNT(user_id) AS no_of_users, user_category
FROM t1
GROUP BY user_category;

--This query creates a view that calculates the total revenue for each user. 

CREATE VIEW user_total_revenue AS
SELECT
user_id,
SUM(price) AS total_revenue
FROM
sales
GROUP BY user_id;--- View can be run using SELECT * FROM user_total_revenue; 52042 rows impacted


-- This query shows the total sales for each season, considering that January to March is winter,April to June is spring, July to September is summer, and October to December is autumn.

SELECT season, SUM(total_price) AS total_sales_by_season
FROM (SELECT
	  EXTRACT(MONTH FROM event_time) AS month,
      SUM(price) AS total_price,
      CASE WHEN EXTRACT(MONTH FROM event_time) IN (1, 2, 3) THEN 'Winter'
           WHEN EXTRACT(MONTH FROM event_time) IN (4, 5, 6) THEN 'Spring'
           WHEN EXTRACT(MONTH FROM event_time) IN (7, 8, 9) THEN 'Summer'
           WHEN EXTRACT(MONTH FROM event_time) IN (10, 11, 12) THEN 'Fall'
      ELSE 'Unknown' END AS season
	  FROM SALES
	  GROUP BY month, season
) AS seasonal_data
GROUP BY season
ORDER BY total_sales_by_season;

-- This query shows the total of products for each season.

SELECT season, COUNT(DISTINCT product_id) AS num_products
FROM (SELECT
      EXTRACT(MONTH FROM event_time) AS month,
      product_id,
      CASE WHEN EXTRACT(MONTH FROM event_time) IN (1, 2, 3) THEN 'Winter'
           WHEN EXTRACT(MONTH FROM event_time) IN (4, 5, 6) THEN 'Spring'
           WHEN EXTRACT(MONTH FROM event_time) IN (7, 8, 9) THEN 'Summer'
           WHEN EXTRACT(MONTH FROM event_time) IN (10, 11, 12) THEN 'Fall'
      ELSE 'Unknown' END AS season
	  FROM SALES
) AS seasonal_data
GROUP BY season
ORDER BY season;

-- This query shows the number of orders for each month of the year 2020 and the average price of those orders for each month. Here, you can see that although February saw the highest sales, it has the lowest average price throughout the year.

SELECT EXTRACT(MONTH FROM event_time) AS month,
COUNT(DISTINCT order_id) AS num_orders,
AVG(price::numeric) AS average_price_per_order
FROM SALES
GROUP BY month
ORDER BY month;

-- This query shows the top 5 best-selling brands and the respective total sales.

SELECT brand, ROUND(SUM(price)::numeric, 2) AS total_price
FROM SALES
GROUP BY brand
ORDER BY total_price DESC
LIMIT 5;

--To obtain the average price of products by category
SELECT user_id, COUNT(order_id) AS total_orders
FROM sales
WHERE user_id IS NOT NULL 
GROUP BY user_id;	

--To obtain the best-selling products by brand. This gives an idea which category code represents the highest average price. 
SELECT category_code, ROUND(AVG(price::numeric),2) AS average_price
FROM sales
WHERE category_code IS NOT NULL
GROUP BY category_code
ORDER BY average_price DESC
Limit 20;

--This Query summarizes the sales count of each brands and their product ID, then the results are ordered in descending orders based on sales count 
SELECT brand, product_id, COUNT(order_id) AS sales_count
FROM sales
WHERE brand IS NOT NULL
GROUP BY brand, product_id
ORDER BY sales_count DESC
Limit 20;