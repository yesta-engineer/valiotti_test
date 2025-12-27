-- --== DDL ==-- --
-- --== основная таблица ==-- --
CREATE TABLE stg_orders (
	order_id int,
	status VARCHAR(50),
	updated_at TIMESTAMP DEFAULT now()
);

-- --== scd2 таблица ==-- --
CREATE TABLE orders_history (
    order_id INT,
    status VARCHAR(50),
    valid_from TIMESTAMP,
    valid_to TIMESTAMP
);

-- --== функция для логики ==-- --
CREATE OR REPLACE FUNCTION scd2_order_status_update()
RETURNS TRIGGER AS $$
DECLARE
    current_version TIMESTAMP; 
BEGIN
    SELECT valid_from INTO current_version
    FROM orders_history
    WHERE order_id = NEW.order_id AND valid_to IS NULL
    FOR UPDATE;

    IF FOUND THEN
        UPDATE orders_history
        SET 
            valid_to = NEW.updated_at
        WHERE order_id = NEW.order_id 
          AND valid_from = current_version;
    END IF;

    INSERT INTO orders_history (order_id, status, valid_from, valid_to)
    VALUES (NEW.order_id, NEW.status, NEW.updated_at, NULL);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- --== триггер. при вставке в stg_order запускается функция ==-- --
CREATE OR REPLACE TRIGGER trg_scd2_order_status
    AFTER INSERT ON stg_orders
    FOR EACH ROW
    EXECUTE FUNCTION scd2_order_status_update();


-- --== DML ==-- --
INSERT INTO stg_orders VALUES 
(194,	'new',	'2025-12-27 16:48:03.161'),
(195,	'new',	'2025-12-27 16:48:22.022'),
(196,	'new',	'2025-12-27 16:44:03.029'),
(194,	'processing',	'2025-12-27 16:50:02.418'),
(194,	'shipped',	'2025-12-27 16:52:02.679'),
(196,	'processing',	'2025-12-27 16:49:02.986'),
(194,	'delivered',	'2025-12-27 16:54:03.282'),
(197,	'new',	'2025-12-27 16:54:03.295'),
(196,	'shipped',	'2025-12-27 16:56:02.451')


-- --== проверка ==-- --
SELECT * FROM stg_orders;
SELECT * FROM orders_history;