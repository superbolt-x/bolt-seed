{{ config (
    alias = target.database + '_googleads_keyword_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}

WITH initial_data as 
    (SELECT date::date as date, keyword_text as keyword, keyword_match_type as match_type, ad_group_name, ad_group_id, campaign_name, campaign_id,
        COALESCE(SUM(cost_micros/1000000),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions,
        COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM(revenue),0) as revenue
    FROM {{ source('googleads_raw', 'keyword_performance_report') }}
    LEFT JOIN 
        (SELECT _fivetran_id,date,keyword_text,keyword_match_type,ad_group_name,ad_group_id,campaign_name,campaign_id,
        CASE WHEN conversion_action_name = 'Purchase (Adwords Pixel)' THEN conversions END as purchases, 
        CASE WHEN conversion_action_name = 'Purchase (Adwords Pixel)' THEN conversions_value END as revenue
        FROM {{ source('googleads_raw', 'keyword_convtype_performance_report') }}) 
    USING (_fivetran_id,date,keyword_text,keyword_match_type,ad_group_name,ad_group_id,campaign_name,campaign_id)
    GROUP BY 1,2,3,4,5,6,7),
    
cleaned_data as 
    (SELECT *, {{ get_date_parts('date') }} FROM initial_data)
    
    {%- for date_granularity in date_granularity_list %}    
    SELECT '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
    keyword, match_type, ad_group_name, ad_group_id, campaign_name, campaign_id,
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
    COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(purchases),0) as purchases, 
    COALESCE(SUM(revenue),0) as revenue
    FROM cleaned_data
    GROUP BY 1,2,3,4,5,6,7,8,9,10
    {% if not loop.last %}UNION ALL
    {% endif %}
{% endfor %}
