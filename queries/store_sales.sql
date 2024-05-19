SELECT store_type, 
	ROUND(SUM(product_price * product_quantity) :: NUMERIC, 2) AS "total_sales",
	ROUND(COUNT(store_type) :: NUMERIC * 100 / (SELECT COUNT(*) FROM orders_table), 2) AS "percentage_total(%)"
FROM orders_table
INNER JOIN dim_store_details USING(store_code)
INNER JOIN dim_products USING(product_code)
GROUP BY store_type
ORDER BY total_sales DESC;