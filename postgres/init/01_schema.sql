-- =============================================================================
-- OLTP source schema (e-commerce). Constraints are intentionally "weak"
-- (like many legacy systems) so that bad data can reach the CDC stream and be
-- caught by the Data Quality layer before Silver.
-- =============================================================================

CREATE TABLE IF NOT EXISTS customers (
    customer_id  BIGSERIAL PRIMARY KEY,
    full_name    VARCHAR(200),
    email        VARCHAR(255),
    phone        VARCHAR(30),
    city         VARCHAR(100),
    status       VARCHAR(20)  DEFAULT 'active',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
    product_id   BIGSERIAL PRIMARY KEY,
    sku          VARCHAR(50),
    name         VARCHAR(200),
    category     VARCHAR(50),
    price        NUMERIC(12, 2),
    is_active    BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id       BIGSERIAL PRIMARY KEY,
    customer_id    BIGINT REFERENCES customers (customer_id),
    status         VARCHAR(20),
    payment_method VARCHAR(30),
    total_amount   NUMERIC(14, 2) DEFAULT 0,
    order_ts       TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id  BIGSERIAL PRIMARY KEY,
    order_id       BIGINT REFERENCES orders (order_id) ON DELETE CASCADE,
    product_id     BIGINT,
    quantity       INTEGER,
    unit_price     NUMERIC(12, 2),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status   ON orders (status);
CREATE INDEX IF NOT EXISTS idx_items_order     ON order_items (order_id);

-- Keep updated_at fresh on every UPDATE --------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_products_updated  BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_orders_updated    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- CDC: full before-image on UPDATE/DELETE ------------------------------------
ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE products    REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

-- Publication used by Debezium's pgoutput plugin ------------------------------
CREATE PUBLICATION dbz_publication FOR TABLE customers, products, orders, order_items;

-- Least-privilege replication user for Debezium ------------------------------
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'dbz_password';
GRANT CONNECT ON DATABASE shop TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
