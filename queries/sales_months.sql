SELECT ROUND(SUM(product_price * product_quantity) :: NUMERIC, 2) AS "total_sales", 
	month
FROM orders_table
INNER JOIN dim_date_times USING(date_uuid)
INNER JOIN dim_products USING(product_code)
GROUP BY month
ORDER BY total_sales DESC
LIMIT 6;