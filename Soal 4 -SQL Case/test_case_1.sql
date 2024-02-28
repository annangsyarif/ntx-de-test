SELECT 
    channelGrouping
    , country
    , totalRevenue
FROM
    (SELECT 
        channelGrouping,
            country,
            SUM(totalTransactionRevenue) AS totalRevenue,
            ROW_NUMBER() OVER (PARTITION BY channelGrouping ORDER BY SUM(totalTransactionRevenue) DESC) AS rn
    FROM
        ecommerce_session_bigquery
    WHERE
    	totalTransactionRevenue IS NOT NULL
    GROUP BY 
    	channelGrouping 
    	, country
    ) AS ranked
WHERE
    rn <= 5;