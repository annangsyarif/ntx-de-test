SELECT 
    fullVisitorId,
    AVG(timeOnSite) AS avg_timeOnSite,
    AVG(pageviews) AS avg_pageviews,
    AVG(sessionQualityDim) AS avg_sessionQualityDim
FROM 
    ecommerce_session_bigquery
GROUP BY 
    fullVisitorId
HAVING 
    AVG(timeOnSite) > (SELECT AVG(timeOnSite) FROM ecommerce_session_bigquery)
    AND AVG(pageviews) < (SELECT AVG(pageviews) FROM ecommerce_session_bigquery);