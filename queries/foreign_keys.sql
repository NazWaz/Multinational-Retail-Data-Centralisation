ALTER TABLE orders_table
ADD CONSTRAINT date_uuid_fk
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT user_uuid_fk
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT card_number_fk
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);

ALTER TABLE orders_table
ADD CONSTRAINT store_code_fk
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

ALTER TABLE orders_table
ADD CONSTRAINT product_code_fk
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);