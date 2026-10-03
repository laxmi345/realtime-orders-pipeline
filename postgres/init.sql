CREATE TABLE IF NOT EXISTS orders (
    order_id       TEXT,
    customer_id    TEXT,
    category       TEXT,
    city           TEXT,
    payment_method TEXT,
    quantity       INT,
    unit_price     NUMERIC(12,2),
    amount         NUMERIC(12,2),
    event_time     TIMESTAMPTZ,
    ingested_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_orders_event_time ON orders (event_time);

CREATE OR REPLACE VIEW kpis AS
SELECT COUNT(*)::int                          AS total_orders,
       COALESCE(SUM(amount), 0)::float        AS total_revenue,
       COALESCE(AVG(amount), 0)::float        AS avg_order_value,
       COUNT(DISTINCT customer_id)::int       AS unique_customers
FROM orders;

CREATE OR REPLACE VIEW sales_per_minute AS
SELECT date_trunc('minute', event_time) AS minute,
       COUNT(*)::int                    AS orders,
       SUM(amount)::float               AS revenue
FROM orders
WHERE event_time > now() - interval '30 minutes'
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW category_revenue AS
SELECT category, COUNT(*)::int AS orders, SUM(amount)::float AS revenue
FROM orders GROUP BY category ORDER BY revenue DESC;

CREATE OR REPLACE VIEW city_revenue AS
SELECT city, COUNT(*)::int AS orders, SUM(amount)::float AS revenue
FROM orders GROUP BY city ORDER BY revenue DESC;

CREATE OR REPLACE VIEW payment_mix AS
SELECT payment_method, COUNT(*)::int AS orders
FROM orders GROUP BY payment_method;
