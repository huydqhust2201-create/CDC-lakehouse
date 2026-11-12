-- Initial snapshot data (captured by Debezium with op = 'r')

INSERT INTO customers (full_name, email, phone, city, status)
SELECT 'Customer ' || g,
       'customer' || g || '@example.com',
       '+84 9' || lpad((g * 7919 % 100000000)::text, 8, '0'),
       (ARRAY['Ha Noi', 'Ho Chi Minh', 'Da Nang', 'Hai Phong', 'Can Tho'])[1 + g % 5],
       CASE WHEN g % 17 = 0 THEN 'inactive' ELSE 'active' END
FROM generate_series(1, 50) AS g;

INSERT INTO products (sku, name, category, price)
SELECT 'SKU-' || lpad(g::text, 5, '0'),
       (ARRAY['Phone', 'Laptop', 'Headphones', 'Keyboard', 'Mouse', 'Monitor'])[1 + g % 6] || ' Model ' || g,
       (ARRAY['electronics', 'computers', 'audio', 'accessories', 'accessories', 'computers'])[1 + g % 6],
       round((20 + (g * 37 % 980))::numeric, 2)
FROM generate_series(1, 30) AS g;

INSERT INTO orders (customer_id, status, payment_method, order_ts)
SELECT 1 + (g * 13 % 50),
       (ARRAY['pending', 'paid', 'shipped', 'delivered', 'cancelled'])[1 + g % 5],
       (ARRAY['card', 'cod', 'bank_transfer', 'e_wallet'])[1 + g % 4],
       now() - ((g % 10) || ' days')::interval - ((g * 53 % 1440) || ' minutes')::interval
FROM generate_series(1, 120) AS g;

INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT o.order_id,
       p.product_id,
       1 + (o.order_id + i) % 3,
       p.price
FROM orders o
CROSS JOIN generate_series(1, 2) AS i
JOIN products p ON p.product_id = 1 + ((o.order_id * 7 + i * 11) % 30);

UPDATE orders o
SET total_amount = s.total
FROM (SELECT order_id, SUM(quantity * unit_price) AS total FROM order_items GROUP BY order_id) s
WHERE s.order_id = o.order_id;
