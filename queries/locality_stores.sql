SELECT locality, 
	COUNT(store_code) AS "total_no_stores"
FROM dim_store_details
GROUP BY locality
HAVING COUNT(store_code) >= 10
ORDER BY total_no_stores DESC;