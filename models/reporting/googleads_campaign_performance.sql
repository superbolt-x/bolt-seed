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
    when campaign_name ~* 'YT' and campaign_name !~* 'amazon' then 'Youtube'
    when campaign_name ~* 'Shopping' and campaign_name ~* 'Brand' and campaign_name !~* 'amazon' then 'Shopping - Brand'
    when campaign_name ~* 'Shopping' and campaign_name !~* 'Brand' and campaign_name !~* 'amazon' then 'Shopping - Non Brand'
    when (campaign_name ~* 'Performance Max' or campaign_name ~* 'PMax') and campaign_name !~* 'amazon' then 'PMax'
    when campaign_name ~* 'NB' and campaign_name !~* 'amazon' then 'Non Brand'
    when campaign_name ~* 'Brand' and campaign_name !~* 'amazon' then 'Branded'
    else 'Other'
end as campaign_type_custom,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
search_impression_share,
search_budget_lost_impression_share,
search_rank_lost_impression_share
FROM {{ ref('googleads_performance_by_campaign') }}
