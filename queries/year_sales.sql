SELECT ROUND(SUM(product_price * product_quantity) :: NUMERIC, 2) AS "total_sales", 
	year, 
	month 
FROM orders_table
INNER JOIN dim_products USING(product_code)
INNER JOIN dim_date_times USING(date_uuid)
GROUP BY year, 
	month
ORDER BY total_sales DESC
LIMIT 10;