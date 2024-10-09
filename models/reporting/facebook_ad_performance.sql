{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}
  
SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
CASE WHEN campaign_name ~* 'Prospect' OR campaign_name ~* 'Interest' THEN 'Campaign Type: Prospecting'
    WHEN campaign_name ~* 'Retarget' THEN 'Campaign Type: Retargeting'
END as campaign_type_default,
case 
    when campaign_name ~* 'DS01' then 'DS01'
    when campaign_name ~* 'VS01' then 'VS01'
    when campaign_name ~* 'PDS08' then 'PDS08'
    when campaign_name ~* 'HCP' then 'HCP'
    else 'Other'
end as product,  
adset_name,
adset_id,
adset_effective_status,
audience,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
purchases,
revenue,
"offsite_conversion.custom.975370734291959" as "VS-01 WK",
onsite_web_lead as leads
FROM {{ ref('facebook_performance_by_ad') }}
