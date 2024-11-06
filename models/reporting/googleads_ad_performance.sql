{{ config (
    alias = target.database + '_googleads_ad_performance'
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
ad_group_name,
ad_group_id,
date,
date_granularity,
spend,
impressions,
clicks,
purchaseadwordspixel as purchases,
purchaseadwordspixel_value as revenue,
checkout_initiated
FROM {{ ref('googleads_performance_by_ad') }}
LEFT JOIN 
    (SELECT DATE_TRUNC('day',date) as date, 'day' as date_granularity,
        customer_id as account_id, ad_group_id, ad_group_name, campaign_id, campaign_name,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Checkout Started' THEN conversions END),0) as checkout_initiated
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6,7
    UNION ALL
    SELECT DATE_TRUNC('week',date) as date, 'week' as date_granularity,
        customer_id as account_id, ad_group_id, ad_group_name, campaign_id, campaign_name,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Checkout Started' THEN conversions END),0) as checkout_initiated
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6,7
    UNION ALL
    SELECT DATE_TRUNC('month',date) as date, 'month' as date_granularity,
        customer_id as account_id, ad_group_id, ad_group_name, campaign_id, campaign_name,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Checkout Started' THEN conversions END),0) as checkout_initiated
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6,7
    UNION ALL
    SELECT DATE_TRUNC('quarter',date) as date, 'quarter' as date_granularity,
        customer_id as account_id, ad_group_id, ad_group_name, campaign_id, campaign_name,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Checkout Started' THEN conversions END),0) as checkout_initiated
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6,7
    UNION ALL
    SELECT DATE_TRUNC('year',date) as date, 'year' as date_granularity,
        customer_id as account_id, ad_group_id, ad_group_name, campaign_id, campaign_name,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Checkout Started' THEN conversions END),0) as checkout_initiated
    FROM {{ source('googleads_raw','ad_convtype_performance_report') }}
    GROUP BY 1,2,3,4,5,6,7
    ) USING (date, date_granularity, account_id, ad_group_id, ad_group_name, campaign_id, campaign_name)
