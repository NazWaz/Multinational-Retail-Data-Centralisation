SELECT COUNT(product_code) AS "numbers_of_sales", 
	SUM(product_quantity) AS "product_quantity_count", 
	CASE WHEN store_type = 'Web Portal' THEN 'Web' ELSE 'Offline' END AS "location"
FROM orders_table
INNER JOIN dim_store_details USING(store_code)
INNER JOIN dim_products USING(product_code)
GROUP BY CASE WHEN store_type = 'Web Portal' THEN 'Web' ELSE 'Offline' END
ORDER BY COUNT(product_code);