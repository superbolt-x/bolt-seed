{{ config (
    alias = target.database + '_googleads_campaign_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
case 
    when campaign_name ~* 'INTL' then 'INTL'
    else 'US'
end as country,
case 
    when campaign_name ~* 'DS01' then 'DS01'
    when campaign_name ~* 'VS01' then 'VS01'
    when campaign_name ~* 'PDS08' then 'PDS08'
    else 'Other'
end as product,
case 
    when campaign_name ~* 'amazon' then 'Amazon'
    when campaign_name ~* 'YT' then 'Youtube'
    when campaign_name ~* 'Shopping' and campaign_name ~* 'Brand' then 'Shopping - Brand'
    when campaign_name ~* 'Shopping' and campaign_name !~* 'Brand' then 'Shopping - Non Brand'
    when campaign_name ~* 'Performance Max' or campaign_name ~* 'PMax' then 'PMax'
    when campaign_name ~* 'NB' then 'Non Brand'
    when campaign_name ~* 'Brand' then 'Branded'
    else 'Other'
end as campaign_type_custom,
date,
date_granularity,
spend,
impressions,
clicks,
purchaseadwordspixel as purchases,
purchaseadwordspixel_value as revenue,
search_impression_share,
search_budget_lost_impression_share,
search_rank_lost_impression_share,
checkout_initiated
FROM {{ ref('googleads_performance_by_campaign') }}
LEFT JOIN 
    (SELECT DATE_TRUNC('day',date) as date, 'day' as date_granularity,
        customer_id as account_id, id as campaign_id, name as campaign_name,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Checkout Started' THEN conversions END),0) as checkout_initiated
    FROM {{ source('googleads_raw','campaign_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5
    UNION ALL
    SELECT DATE_TRUNC('week',date) as date, 'week' as date_granularity,
        customer_id as account_id, id as campaign_id, name as campaign_name,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Checkout Started' THEN conversions END),0) as checkout_initiated
    FROM {{ source('googleads_raw','campaign_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5
    UNION ALL
    SELECT DATE_TRUNC('month',date) as date, 'month' as date_granularity,
        customer_id as account_id, id as campaign_id, name as campaign_name,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Checkout Started' THEN conversions END),0) as checkout_initiated
    FROM {{ source('googleads_raw','campaign_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5
    UNION ALL
    SELECT DATE_TRUNC('quarter',date) as date, 'quarter' as date_granularity,
        customer_id as account_id, id as campaign_id, name as campaign_name,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Checkout Started' THEN conversions END),0) as checkout_initiated
    FROM {{ source('googleads_raw','campaign_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5
    UNION ALL
    SELECT DATE_TRUNC('year',date) as date, 'year' as date_granularity,
        customer_id as account_id, id as campaign_id, name as campaign_name,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Checkout Started' THEN conversions END),0) as checkout_initiated
    FROM {{ source('googleads_raw','campaign_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5
    ) USING (date, date_granularity, account_id, campaign_id, campaign_name)
