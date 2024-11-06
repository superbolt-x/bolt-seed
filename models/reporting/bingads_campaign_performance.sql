{{ config (
    alias = target.database + '_bingads_campaign_performance'
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
    when campaign_name ~* 'Shopping' and campaign_name ~* 'Brand' then 'Shopping - Brand'
    when campaign_name ~* 'Shopping' and campaign_name !~* 'Brand' then 'Shopping - Non Brand'
    when campaign_name ~* 'NB' then 'Non Brand'
    when campaign_name ~* 'Brand' then 'Branded'
    else 'Other'
end as campaign_type_custom,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
revenue,
view_through_conversions
FROM {{ ref('bingads_performance_by_campaign') }}
