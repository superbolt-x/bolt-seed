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
    when campaign_name ~* 'YT' and campaign_name !~* 'amazon' then 'Youtube'
    when campaign_name ~* 'Shopping' and campaign_name ~* 'Brand' and campaign_name !~* 'amazon' then 'Shopping - Brand'
    when campaign_name ~* 'Shopping' and campaign_name !~* 'Brand' and campaign_name !~* 'amazon' then 'Shopping - Non Brand'
    when (campaign_name ~* 'Performance Max' or campaign_name ~* 'PMax') and campaign_name !~* 'amazon' then 'PMax'
    when campaign_name ~* 'NB' and campaign_name !~* 'amazon' then 'Non Brand'
    when campaign_name ~* 'Brand' and campaign_name !~* 'amazon' then 'Branded'
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
purchaseadwordspixel_value as revenue
FROM {{ ref('googleads_performance_by_ad') }}
