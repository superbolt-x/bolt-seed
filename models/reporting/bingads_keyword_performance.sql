{{ config (
    alias = target.database + '_bingads_keyword_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
  
WITH initial_data as 
  (SELECT date::date as date, kp.ad_group_id, campaign_id, name as keyword, match_type, 
    COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(conversions),0) as purchases, 
    COALESCE(SUM(revenue),0) as revenue
  FROM {{ source('bingads_raw', 'keyword_performance_daily_report') }} kp
  LEFT JOIN {{ source('bingads_raw', 'keyword_history') }} kh ON kp.keyword_id = kh.id AND kp.ad_group_id = kh.ad_group_id
  GROUP BY 1,2,3,4,5),
    
cleaned_data as
  (SELECT *, {{ get_date_parts('date') }} FROM initial_data)
  
{%- for date_granularity in date_granularity_list %}    
  SELECT '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
    keyword, match_type, ad_group_id, campaign_name, campaign_id,
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
    COALESCE(SUM(spend),0) as spend, 
    COALESCE(SUM(impressions),0) as impressions, 
    COALESCE(SUM(clicks),0) as clicks, 
    COALESCE(SUM(purchases),0) as purchases, 
    COALESCE(SUM(revenue),0) as revenue
  FROM cleaned_data
  LEFT JOIN (SELECT DISTINCT campaign_id, campaign_name FROM {{ source('reporting', 'bingads_campaign_performance') }}) USING (campaign_id)
  GROUP BY 1,2,3,4,5,6,7,8,9,10
  {% if not loop.last %}UNION ALL
  {% endif %}
{% endfor %}
