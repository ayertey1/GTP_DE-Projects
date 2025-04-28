CREATE TABLE IF NOT EXISTS user_events(
    event_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    product_id INT NOT NULL,
    product_category VARCHAR(50),
    event_time TIMESTAMP NOT NULL
);