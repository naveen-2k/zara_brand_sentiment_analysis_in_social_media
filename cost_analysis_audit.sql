SELECT 
    identity_metadata.owned_by AS user_email,
    usage_date,
    sku_name,
    SUM(usage_quantity) AS total_dbu_used,
    CASE 
        WHEN sku_name LIKE '%ALL_PURPOSE_COMPUTE%' THEN SUM(usage_quantity) * 0.15  -- Standard pricing
        WHEN sku_name LIKE '%SERVERLESS_SQL%' THEN SUM(usage_quantity) * 0.22  -- Serverless SQL pricing
        ELSE SUM(usage_quantity) * 0.10  -- Default for other workloads
    END AS estimated_cost_usd
FROM system.billing.usage
WHERE identity_metadata.owned_by IS NOT NULL 
GROUP BY user_email, usage_date, sku_name
ORDER BY usage_date DESC ;
