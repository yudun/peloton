CREATE TABLE c1 (
    a integer,
    b integer,
    c integer, CHECK (a * b > c + 1)
);

INSERT INTO c1 VALUES (0, 1, 2);
INSERT INTO c1 VALUES (8, 1, 2);
INSERT INTO c1 VALUES (1, 2, 1);
SELECT * FROM c1;

CREATE TABLE c2 (
    product_no integer PRIMARY KEY,
    name text,
    price numeric, CHECK (price > 10),
    discounted_price numeric,
    CONSTRAINT valid_discount CHECK (price > discounted_price AND discounted_price > 0)
);

INSERT INTO c2 VALUES (1, 'mavis', 100, 99);
INSERT INTO c2 VALUES (2, 'shimin', 10, 1);
SELECT * FROM c2;

INSERT INTO c2 VALUES (2, 'shimin', 100, 101);
INSERT INTO c2 VALUES (2, 'shimin', 120, 101);
SELECT * FROM c2;

INSERT INTO c2 VALUES (3, 'lei', 30, -10);
INSERT INTO c2 VALUES (3, 'lei', 30, 10);
INSERT INTO c2 VALUES (3, 'lei', 40, 10);
SELECT * FROM c2;


