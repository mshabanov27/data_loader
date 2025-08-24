CREATE TABLE apps(
    app_apple_id BIGINT PRIMARY KEY NOT NULL,
    app_name TEXT NOT NULL,
    created_at TIMESTAMP(0) WITH TIME zone NOT NULL,
    modified_at TIMESTAMP(0) WITH TIME zone
);

CREATE TABLE subscriptions(
    subscription_apple_id BIGINT PRIMARY KEY NOT NULL,
    subscription_group_id BIGINT NOT NULL,
    subscription_name TEXT NOT NULL,
    subscription_duration TEXT NOT NULL,
    created_at TIMESTAMP(0) WITH TIME zone NOT NULL,
    modified_at TIMESTAMP(0) WITH TIME zone
);

CREATE TABLE subscribers(
    subscriber_apple_id BIGINT PRIMARY KEY NOT NULL,
    subscriber_id_reset BOOLEAN NOT NULL,
    created_at TIMESTAMP(0) WITH TIME zone NOT NULL,
    modified_at TIMESTAMP(0) WITH TIME zone
);

CREATE TABLE orders(
    id SERIAL PRIMARY KEY NOT NULL,
    order_date DATE NOT NULL,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(subscriber_apple_id),
    app_id BIGINT NOT NULL REFERENCES apps(app_apple_id),
    subscription_id BIGINT NOT NULL REFERENCES subscriptions(subscription_apple_id),
    is_trial BOOLEAN NOT NULL,
    trial_duration TEXT,
    customer_price FLOAT NOT NULL,
    customer_currency VARCHAR(3) NOT NULL,
    developer_proceeds FLOAT NOT NULL,
    proceeds_currency VARCHAR(3) NOT NULL,
    country VARCHAR(2),
    device TEXT,
    marketing_opt_in_duration TEXT,
    preserved_pricing TEXT,
    proceeds_reason TEXT,
    client TEXT,
    created_at TIMESTAMP(0) WITH TIME zone NOT NULL,
    modified_at TIMESTAMP(0) WITH TIME zone
);

CREATE TABLE refunds(
    id SERIAL PRIMARY KEY NOT NULL,
    refund_date DATE NOT NULL,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(subscriber_apple_id),
    app_id BIGINT NOT NULL REFERENCES apps(app_apple_id),
    subscription_id BIGINT NOT NULL REFERENCES subscriptions(subscription_apple_id),
    customer_price FLOAT NOT NULL,
    customer_currency VARCHAR(3) NOT NULL,
    developer_proceeds FLOAT NOT NULL,
    proceeds_currency VARCHAR(3) NOT NULL,
    original_purchase_date DATE NOT NULL,
    country VARCHAR(2),
    device TEXT,
    marketing_opt_in_duration TEXT,
    preserved_pricing TEXT,
    proceeds_reason TEXT,
    client TEXT,
    created_at TIMESTAMP(0) WITH TIME zone NOT NULL,
    modified_at TIMESTAMP(0) WITH TIME zone
);


CREATE OR REPLACE FUNCTION set_timestamps()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.created_at IS NULL THEN
            NEW.created_at := NOW();
        END IF;
    ELSIF TG_OP = 'UPDATE' THEN
        IF ROW(NEW.*) IS DISTINCT FROM ROW(OLD.*) THEN
            NEW.modified_at := NOW();
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER apps_set_timestamps
BEFORE INSERT OR UPDATE ON apps
FOR EACH ROW EXECUTE FUNCTION set_timestamps();

CREATE TRIGGER subscriptions_set_timestamps
BEFORE INSERT OR UPDATE ON subscriptions
FOR EACH ROW EXECUTE FUNCTION set_timestamps();

CREATE TRIGGER orders_set_timestamps
BEFORE INSERT OR UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION set_timestamps();

CREATE TRIGGER refunds_set_timestamps
BEFORE INSERT OR UPDATE ON refunds
FOR EACH ROW EXECUTE FUNCTION set_timestamps();

CREATE TRIGGER subscribers_set_timestamps
BEFORE INSERT OR UPDATE ON subscribers
FOR EACH ROW EXECUTE FUNCTION set_timestamps();
