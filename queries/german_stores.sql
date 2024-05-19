SELECT ROUND(SUM(product_price * product_quantity) :: NUMERIC, 2) AS "total_sales", 
	store_type, 
	country_code
FROM orders_table
INNER JOIN dim_products USING(product_code)
INNER JOIN dim_store_details USING(store_code)
WHERE country_code = 'DE'
GROUP BY store_type, country_code
ORDER BY total_sales;