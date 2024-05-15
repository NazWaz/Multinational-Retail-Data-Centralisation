ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(22);

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(5);

ALTER TABLE dim_card_details
ALTER COLUMN card_provider TYPE VARCHAR(255);

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE;