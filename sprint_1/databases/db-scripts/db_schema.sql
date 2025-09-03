CREATE TABLE products (
    product_id UUID PRIMARY KEY,
    type TEXT NOT NULL,
    size TEXT,
    flavour TEXT,
    price NUMERIC(10,2) NOT NULL
);

CREATE TABLE branches (
    branch_id UUID PRIMARY KEY,
    branch_name TEXT UNIQUE NOT NULL
);

CREATE TABLE orders (
    order_id UUID NOT NULL,
    branch_id UUID NOT NULL REFERENCES branches(branch_id),
    product_id UUID NOT NULL REFERENCES products(product_id),
    quantity INT NOT NULL DEFAULT 1,
    order_date TIMESTAMP NOT NULL,
    total_price NUMERIC(10,2) NOT NULL,
    payment_method TEXT NOT NULL,
    PRIMARY KEY(order_id, product_id)
);
