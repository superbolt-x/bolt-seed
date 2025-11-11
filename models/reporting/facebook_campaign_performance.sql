{{ config (
    alias = target.database + '_facebook_campaign_performance'
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
    when campaign_name ~* 'HCP' THEN 'HCP'
    when campaign_name ~* 'DM02' THEN 'DM02'
	when campaign_name ~* 'AM02' THEN 'AM02'
    when campaign_name ~* 'PM02' THEN 'PM02'
    else 'Other'
end as product,  
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
FROM {{ ref('facebook_performance_by_campaign') }}
