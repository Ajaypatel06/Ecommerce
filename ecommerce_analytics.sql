CREATE DATABASE IF NOT EXISTS ecommerce_analytics;
USE ecommerce_analytics;



SET GLOBAL max_allowed_packet = 1073741824;
SET GLOBAL net_read_timeout   = 600;
SET GLOBAL net_write_timeout  = 600;
SET GLOBAL wait_timeout       = 7200;
SET GLOBAL interactive_timeout= 7200;



CREATE TABLE customers (
  customer_id INT PRIMARY KEY,
  location VARCHAR(100),
  segment VARCHAR(20),
  signup_date DATETIME,
  INDEX ix_customers_location (location),
  INDEX ix_customers_segment (segment)
);

CREATE TABLE products (
  product_id INT PRIMARY KEY,
  category VARCHAR(100),
  sub_category VARCHAR(100),
  brand VARCHAR(100),
  price DECIMAL(18,2),
  stock INT,
  INDEX ix_products_category (category),
  INDEX ix_products_brand (brand)
);

CREATE TABLE orders (
  order_id BIGINT PRIMARY KEY,
  order_date DATETIME,
  customer_id INT,
  product_id INT,
  quantity INT,
  revenue DECIMAL(18,2),
  cost DECIMAL(18,2),
  discount DECIMAL(18,2),
  payment_type VARCHAR(30),
  channel VARCHAR(30),
  INDEX ix_orders_date (order_date),
  INDEX ix_orders_customer (customer_id),
  INDEX ix_orders_product (product_id),
  INDEX ix_orders_channel (channel)
);

CREATE TABLE shipments (
  shipment_id BIGINT PRIMARY KEY,
  order_id BIGINT,
  ship_date DATE,
  delivery_date DATE,
  status VARCHAR(30),
  courier VARCHAR(100),
  shipping_cost DECIMAL(18,2),
  INDEX ix_shipments_order (order_id),
  INDEX ix_shipments_ship_date (ship_date)
);

CREATE TABLE returns (
  return_id BIGINT PRIMARY KEY,
  order_id BIGINT,
  return_date DATE,
  reason VARCHAR(200),
  refund_amount DECIMAL(18,2),
  INDEX ix_returns_order (order_id),
  INDEX ix_returns_date (return_date)
);

CREATE TABLE sessions (
  session_id BIGINT PRIMARY KEY,
  customer_id INT,
  visit_date DATE,
  device VARCHAR(30),
  channel VARCHAR(30),
  pages_viewed INT,
  time_spent_sec INT,
  conversion_flag TINYINT,
  INDEX ix_sessions_customer (customer_id),
  INDEX ix_sessions_date (visit_date),
  INDEX ix_sessions_channel (channel)
);

select * from customers;
select * from orders;
select count(*) from orders;


-- CUSTOMERS
TRUNCATE TABLE customers;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ecommerce/customers.csv'
INTO TABLE customers
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(customer_id, location, segment, @signup_date)
SET signup_date = NULLIF(@signup_date,'');

-- PRODUCTS
TRUNCATE TABLE products;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ecommerce/products.csv'
INTO TABLE products
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(product_id, category, sub_category, brand, @price, @stock)
SET price = NULLIF(@price,''), stock = NULLIF(@stock,'');

-- ORDERS
TRUNCATE TABLE orders;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ecommerce/orders.csv'
INTO TABLE orders
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(order_id, @order_date, customer_id, product_id, @quantity, @revenue, @cost, @discount, payment_type, channel)
SET order_date = NULLIF(@order_date,''),
    quantity   = NULLIF(@quantity,''),
    revenue    = NULLIF(@revenue,''),
    cost       = NULLIF(@cost,''),
    discount   = NULLIF(@discount,'');

-- SHIPMENTS
TRUNCATE TABLE shipments;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ecommerce/shipments.csv'
INTO TABLE shipments
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(shipment_id, order_id, @ship_date, @delivery_date, status, courier, @shipping_cost)
SET ship_date     = NULLIF(@ship_date,''),
    delivery_date = NULLIF(@delivery_date,''),
    shipping_cost = NULLIF(@shipping_cost,'');

-- RETURNS
TRUNCATE TABLE returns;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ecommerce/returns.csv'
INTO TABLE returns
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(return_id, order_id, @return_date, reason, @refund_amount)
SET return_date   = NULLIF(@return_date,''),
    refund_amount = NULLIF(@refund_amount,'');

-- SESSIONS
TRUNCATE TABLE sessions;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ecommerce/sessions.csv'
INTO TABLE sessions
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(session_id, customer_id, @visit_date, device, channel, @pages_viewed, @time_spent_sec, @conversion_flag)
SET visit_date      = NULLIF(@visit_date,''),
    pages_viewed    = NULLIF(@pages_viewed,''),
    time_spent_sec  = NULLIF(@time_spent_sec,''),
    conversion_flag = NULLIF(@conversion_flag,'');



SELECT COUNT(*) AS orders_rows     FROM orders;
SELECT COUNT(*) AS shipments_rows  FROM shipments;
SELECT COUNT(*) AS sessions_rows   FROM sessions;

-- quick sanity on date ranges
SELECT MIN(order_date), MAX(order_date) FROM orders;
SELECT MIN(ship_date),  MAX(ship_date)  FROM shipments;
SELECT MIN(visit_date), MAX(visit_date) FROM sessions;


-- QC
SELECT 'customers', COUNT(*) FROM customers
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'orders', COUNT(*) FROM orders
UNION ALL SELECT 'shipments', COUNT(*) FROM shipments
UNION ALL SELECT 'returns', COUNT(*) FROM returns
UNION ALL SELECT 'sessions', COUNT(*) FROM sessions;

-- Orphans
SELECT 'orders->customers' rel, COUNT(*) orphans
FROM orders o LEFT JOIN customers c ON o.customer_id=c.customer_id
WHERE c.customer_id IS NULL;

SELECT 'orders->products' rel, COUNT(*) orphans
FROM orders o LEFT JOIN products p ON o.product_id=p.product_id
WHERE p.product_id IS NULL;

SELECT 'shipments->orders' rel, COUNT(*) orphans
FROM shipments s LEFT JOIN orders o ON s.order_id=o.order_id
WHERE o.order_id IS NULL;

SELECT 'returns->orders' rel, COUNT(*) orphans
FROM returns r LEFT JOIN orders o ON r.order_id=o.order_id
WHERE o.order_id IS NULL;

SELECT 'sessions->customers' rel, COUNT(*) orphans
FROM sessions s LEFT JOIN customers c ON s.customer_id=c.customer_id
WHERE c.customer_id IS NULL;

-- Duplicates
SELECT 'orders PK dupes' msg, COUNT(*) cnt FROM (
  SELECT order_id FROM orders GROUP BY order_id HAVING COUNT(*)>1
) x;

-- Date sanity
SELECT MIN(order_date) min_order, MAX(order_date) max_order, SUM(order_date IS NULL) nulls FROM orders;



-- Q1: Revenue and orders by month
SELECT DATE_FORMAT(order_date,'%Y-%m') ym, COUNT(*) orders_cnt, SUM(revenue) revenue
FROM orders GROUP BY ym ORDER BY ym;

-- Q2: AOV trend
SELECT DATE_FORMAT(order_date,'%Y-%m') ym, AVG(revenue) aov
FROM orders GROUP BY ym ORDER BY ym;

-- Q3: Gross margin % trend
SELECT DATE_FORMAT(order_date,'%Y-%m') ym,
       (SUM(revenue)-SUM(cost))/NULLIF(SUM(revenue),0) margin_pct
FROM orders GROUP BY ym ORDER BY ym;

-- Q4: Top 20 categories by revenue
SELECT p.category, SUM(o.revenue) revenue
FROM orders o JOIN products p ON o.product_id=p.product_id
GROUP BY p.category ORDER BY revenue DESC LIMIT 20;

-- Q5: Top 20 brands by revenue
SELECT p.brand, SUM(o.revenue) revenue
FROM orders o JOIN products p ON o.product_id=p.product_id
GROUP BY p.brand ORDER BY revenue DESC LIMIT 20;

-- Q6: Discount impact on margin
SELECT DATE_FORMAT(order_date,'%Y-%m') ym,
       AVG(discount/NULLIF(revenue,0)) avg_discount_pct,
       (SUM(revenue)-SUM(cost))/NULLIF(SUM(revenue),0) margin_pct
FROM orders GROUP BY ym ORDER BY ym;

-- Q7: Channel revenue split
SELECT channel, SUM(revenue) revenue, COUNT(*) orders_cnt
FROM orders GROUP BY channel;

-- Q8: Payment type mix
SELECT payment_type, COUNT(*) orders_cnt, SUM(revenue) revenue
FROM orders GROUP BY payment_type ORDER BY orders_cnt DESC;

-- Q9: Conversion rate by channel (same day)
WITH s AS (
  SELECT channel, visit_date, COUNT(*) sessions
  FROM sessions GROUP BY channel, visit_date
),
o AS (
  SELECT channel, DATE(order_date) od, COUNT(*) orders
  FROM orders GROUP BY channel, DATE(order_date)
)
SELECT s.channel, SUM(COALESCE(o.orders,0))/NULLIF(SUM(s.sessions),0) conv_rate
FROM s LEFT JOIN o ON s.channel=o.channel AND s.visit_date=o.od
GROUP BY s.channel;

-- Q10: Conversion rate by device (same day)
WITH s AS (
  SELECT device, visit_date, COUNT(*) sessions
  FROM sessions GROUP BY device, visit_date
),
o AS (
  SELECT DATE(order_date) od, COUNT(*) orders
  FROM orders GROUP BY DATE(order_date)
)
SELECT s.device, SUM(COALESCE(o.orders,0))/NULLIF(SUM(s.sessions),0) conv_rate
FROM s LEFT JOIN o ON s.visit_date=o.od
GROUP BY s.device;

-- Q11: On-time delivery % (delay_days<=0)
SELECT DATE_FORMAT(ship_date,'%Y-%m') ym,
       AVG(DATEDIFF(delivery_date, ship_date) <= 0) on_time_rate
FROM shipments GROUP BY ym ORDER BY ym;

-- Q12: Avg delivery days
SELECT DATE_FORMAT(ship_date,'%Y-%m') ym,
       AVG(DATEDIFF(delivery_date, ship_date)) avg_deliv_days
FROM shipments GROUP BY ym ORDER BY ym;

-- Q13: Return rate % by month
SELECT DATE_FORMAT(o.order_date,'%Y-%m') ym,
       COUNT(r.return_id)/NULLIF(COUNT(o.order_id),0) return_rate
FROM orders o LEFT JOIN returns r ON o.order_id=r.order_id
GROUP BY ym ORDER BY ym;

-- Q14: Return % by category
SELECT p.category,
       COUNT(r.return_id)/NULLIF(COUNT(o.order_id),0) return_rate,
       COUNT(o.order_id) orders_cnt
FROM orders o
JOIN products p ON o.product_id=p.product_id
LEFT JOIN returns r ON o.order_id=r.order_id
GROUP BY p.category ORDER BY return_rate DESC;

-- Q15: Refund leakage (refund / revenue)
SELECT DATE_FORMAT(o.order_date,'%Y-%m') ym,
       SUM(r.refund_amount)/NULLIF(SUM(o.revenue),0) refund_pct
FROM orders o LEFT JOIN returns r ON o.order_id=r.order_id
GROUP BY ym ORDER BY ym;

-- Q16: Avg order frequency per customer
SELECT AVG(cnt) avg_orders_per_customer
FROM (SELECT customer_id, COUNT(*) cnt FROM orders GROUP BY customer_id) x;

-- Q17: Repeat purchase % (>=2 orders)
SELECT AVG(cnt>=2) repeat_rate
FROM (SELECT customer_id, COUNT(*) cnt FROM orders GROUP BY customer_id) x;

-- Q18: CLV proxy by segment
WITH cust_orders AS (
  SELECT c.segment, o.customer_id,
         AVG(o.revenue) aov,
         COUNT(*) n_orders,
         (SUM(o.revenue)-SUM(o.cost))/NULLIF(SUM(o.revenue),0) margin_pct
  FROM orders o JOIN customers c ON o.customer_id=c.customer_id
  GROUP BY c.segment, o.customer_id
)
SELECT segment, AVG(aov*n_orders*margin_pct) clv_proxy
FROM cust_orders GROUP BY segment;

-- Q19: Revenue split by segment
SELECT c.segment, SUM(o.revenue) revenue, COUNT(*) orders_cnt
FROM orders o JOIN customers c ON o.customer_id=c.customer_id
GROUP BY c.segment;

-- Q20: Cohort retention (signup month vs ordering months)
WITH first_order AS (
  SELECT customer_id, MIN(order_date) first_order_date
  FROM orders GROUP BY customer_id
),
coh AS (
  SELECT DATE_FORMAT(c.signup_date,'%Y-%m') cohort, f.customer_id
  FROM customers c JOIN first_order f USING(customer_id)
),
ordm AS (
  SELECT customer_id, DATE_FORMAT(order_date,'%Y-%m') ym
  FROM orders GROUP BY customer_id, DATE_FORMAT(order_date,'%Y-%m')
)
SELECT cohort, ym, COUNT(*) active_customers
FROM coh JOIN ordm USING(customer_id)
GROUP BY cohort, ym ORDER BY cohort, ym;

-- Q21: AOV by channel and device (same day)
SELECT o.channel, s.device, AVG(o.revenue) aov
FROM orders o
JOIN sessions s
  ON s.customer_id=o.customer_id
 AND s.visit_date = DATE(o.order_date)
GROUP BY o.channel, s.device;

-- Q22: Top 20 products by revenue
SELECT p.product_id, p.brand, p.category, p.sub_category, SUM(o.revenue) revenue
FROM orders o JOIN products p ON o.product_id=p.product_id
GROUP BY p.product_id, p.brand, p.category, p.sub_category
ORDER BY revenue DESC LIMIT 20;

-- Q23: Orders by location
SELECT c.location, COUNT(*) orders_cnt, SUM(o.revenue) revenue
FROM orders o JOIN customers c ON o.customer_id=c.customer_id
GROUP BY c.location ORDER BY revenue DESC;

-- Q24: Delivery status distribution
SELECT status, COUNT(*) n FROM shipments GROUP BY status ORDER BY n DESC;

-- Q25: Late delivery impact on returns
SELECT AVG(CASE WHEN DATEDIFF(s.delivery_date, s.ship_date)>0
                THEN r.return_id IS NOT NULL
                ELSE NULL END) AS prob_return_if_late
FROM shipments s LEFT JOIN returns r ON s.order_id=r.order_id;

-- Q26: Refund amount by return reason
SELECT reason, AVG(refund_amount) avg_refund, COUNT(*) n
FROM returns GROUP BY reason ORDER BY avg_refund DESC;

-- Q27: Time-to-delivery P50/P90 (window workaround)
WITH d AS (
  SELECT DATEDIFF(delivery_date, ship_date) d FROM shipments
),
r AS (
  SELECT d,
         ROW_NUMBER() OVER (ORDER BY d) rn,
         COUNT(*) OVER () n
  FROM d
)
SELECT
  (SELECT d FROM r WHERE rn = CEIL(0.50*n)) AS p50_days,
  (SELECT d FROM r WHERE rn = CEIL(0.90*n)) AS p90_days;

-- Q28: Bounce proxy (pages<=2) rate by channel
SELECT channel, AVG(pages_viewed<=2) bounce_rate
FROM sessions GROUP BY channel;

-- Q29: Engagement proxy by device
SELECT device, AVG(time_spent_sec) avg_time_spent
FROM sessions GROUP BY device ORDER BY avg_time_spent DESC;

-- Q30: Price vs discount correlation (corr = cov/σx/σy)
SELECT
  (COVAR_POP(price, discount) /
   NULLIF(STDDEV_POP(price),0) /
   NULLIF(STDDEV_POP(discount),0)) AS price_discount_corr
FROM (
  SELECT o.discount AS discount, p.price AS price
  FROM orders o JOIN products p ON o.product_id=p.product_id
) x;

-- Q31: Churn proxy: no orders in last 90 days
WITH last_order AS (
  SELECT customer_id, MAX(order_date) last_dt
  FROM orders GROUP BY customer_id
)
SELECT AVG(DATEDIFF(CURDATE(), last_dt) > 90) churn_proxy FROM last_order;

-- Q32: Refund % by category
SELECT p.category,
       SUM(r.refund_amount)/NULLIF(SUM(o.revenue),0) refund_pct
FROM orders o JOIN products p ON o.product_id=p.product_id
LEFT JOIN returns r ON o.order_id=r.order_id
GROUP BY p.category ORDER BY refund_pct DESC;

/* ---------- BI VIEWS ---------- */

CREATE OR REPLACE VIEW vw_sales_monthly AS
SELECT DATE_FORMAT(order_date,'%Y-%m') ym,
       COUNT(*) orders_cnt,
       SUM(revenue) revenue,
       SUM(cost) cost,
       (SUM(revenue)-SUM(cost))/NULLIF(SUM(revenue),0) margin_pct,
       AVG(revenue) aov
FROM orders
GROUP BY DATE_FORMAT(order_date,'%Y-%m');

CREATE OR REPLACE VIEW vw_conversion_by_channel AS
WITH s AS (
  SELECT channel, visit_date, COUNT(*) sessions
  FROM sessions GROUP BY channel, visit_date
),
o AS (
  SELECT channel, DATE(order_date) od, COUNT(*) orders
  FROM orders GROUP BY channel, DATE(order_date)
)
SELECT s.channel,
       SUM(COALESCE(o.orders,0)) orders,
       SUM(s.sessions) sessions,
       SUM(COALESCE(o.orders,0))/NULLIF(SUM(s.sessions),0) conv_rate
FROM s LEFT JOIN o ON s.channel=o.channel AND s.visit_date=o.od
GROUP BY s.channel;

CREATE OR REPLACE VIEW vw_returns_by_category AS
SELECT p.category,
       COUNT(r.return_id)/NULLIF(COUNT(o.order_id),0) return_rate,
       SUM(r.refund_amount)/NULLIF(SUM(o.revenue),0) refund_pct
FROM orders o JOIN products p ON o.product_id=p.product_id
LEFT JOIN returns r ON o.order_id=r.order_id
GROUP BY p.category;

CREATE OR REPLACE VIEW vw_delivery_kpis_monthly AS
SELECT DATE_FORMAT(ship_date,'%Y-%m') ym,
       AVG(DATEDIFF(delivery_date, ship_date) <= 0) on_time_rate,
       AVG(DATEDIFF(delivery_date, ship_date))       avg_delivery_days
FROM shipments
GROUP BY DATE_FORMAT(ship_date,'%Y-%m');

CREATE OR REPLACE VIEW vw_customer_cohorts AS
WITH first_order AS (
  SELECT customer_id, MIN(order_date) first_order_date
  FROM orders GROUP BY customer_id
),
coh AS (
  SELECT DATE_FORMAT(c.signup_date,'%Y-%m') cohort, f.customer_id
  FROM customers c JOIN first_order f USING(customer_id)
),
ordm AS (
  SELECT customer_id, DATE_FORMAT(order_date,'%Y-%m') ym
  FROM orders GROUP BY customer_id, DATE_FORMAT(order_date,'%Y-%m')
)
SELECT cohort, ym, COUNT(*) active_customers
FROM coh JOIN ordm USING(customer_id)
GROUP BY cohort, ym;
