UPDATE dim_store_details
SET address = NULL, longitude = NULL, locality = NULL
WHERE address = 'N/A';

ALTER TABLE dim_store_details
ALTER COLUMN address TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT
USING longitude :: NUMERIC(15, 5);

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT
USING staff_numbers :: SMALLINT;

ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE;

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT
USING latitude :: NUMERIC(15, 5);

ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255);