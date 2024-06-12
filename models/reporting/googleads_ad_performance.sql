{{ config (
    alias = target.database + '_googleads_ad_performance'
)}}

SELECT
account_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
ad_group_name,
ad_group_id,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
"offsite_conversion.custom.975370734291959" as "VS-01 WK"
FROM {{ ref('googleads_performance_by_ad') }}
