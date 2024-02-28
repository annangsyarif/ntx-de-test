SELECT 
    v2ProductName
    , SUM(productRevenue) AS total_revenue
    , SUM(productQuantity) AS total_quantity_sold
    , SUM(productRefundAmount) AS total_refund_amount
    , SUM(productRevenue) - SUM(productRefundAmount) AS net_revenue
    , CASE 
        WHEN (SUM(productRefundAmount) / SUM(productRevenue)) > 0.1 THEN 'Yes'
        ELSE 'No'
    END AS refund_amount_exceeds_10_percent
FROM 
    ecommerce_session_bigquery
WHERE
	transactions IS NOT NULL
GROUP BY 
    v2ProductName
ORDER BY 
    net_revenue DESC
    , SUM(productQuantity) desc;