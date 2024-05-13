ALTER TABLE dim_products
ALTER COLUMN product_name TYPE VARCHAR(255);

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT;

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT;

ALTER TABLE dim_products
ALTER COLUMN category TYPE VARCHAR(255);

ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(17);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE;

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID
USING uuid :: UUID;

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

UPDATE dim_products
SET still_available = 'Yes'
WHERE still_available = 'Still available';

UPDATE dim_products
SET still_available = 'No'
WHERE still_available = 'Removed';

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOL
USING still_available :: BOOLEAN;

ALTER TABLE dim_products 
ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(14);