{{ config (
    alias = target.database + '_googleads_keyword_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}

WITH initial_data as 
    (SELECT date::date as date, keyword_text as keyword, keyword_match_type as match_type, ad_group_name, ad_group_id, campaign_name, campaign_id,
        COALESCE(SUM(cost_micros/1000000),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions,
        COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM(revenue),0) as revenue, COALESCE(SUM(add_to_cart),0) as add_to_cart
    FROM {{ source('googleads_raw', 'keyword_performance_report') }}
    LEFT JOIN 
        (SELECT date,keyword_text,keyword_match_type,ad_group_name,ad_group_id,campaign_name,campaign_id,
        COALESCE(SUM(CASE WHEN conversion_action_name ~* 'Purchase' AND conversion_action_name ~* 'Adwords' THEN conversions END),0) as purchases, 
        COALESCE(SUM(CASE WHEN conversion_action_name ~* 'Purchase' AND conversion_action_name ~* 'Adwords' THEN conversions_value END),0) as revenue,
        COALESCE(SUM(CASE WHEN conversion_action_name = 'Add to cart' THEN conversions END),0) as add_to_cart
        FROM {{ source('googleads_raw', 'keyword_convtype_performance_report') }}
        GROUP BY 1,2,3,4,5,6,7) 
    USING (date,keyword_text,keyword_match_type,ad_group_name,ad_group_id,campaign_name,campaign_id)
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
        when campaign_name ~* 'amazon' then 'Amazon'
        when campaign_name ~* 'YT' then 'Youtube'
        when campaign_name ~* 'Shopping' and campaign_name ~* 'Brand' then 'Shopping - Brand'
        when campaign_name ~* 'Shopping' and campaign_name !~* 'Brand' then 'Shopping - Non Brand'
        when campaign_name ~* 'Performance Max' or campaign_name ~* 'PMax' then 'PMax'
        when campaign_name ~* 'NB' then 'Non Brand'
        when campaign_name ~* 'Brand' then 'Branded'
        else 'Other'
    end as campaign_type_custom,
    COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(purchases),0) as purchases, 
    COALESCE(SUM(revenue),0) as revenue, COALESCE(SUM(add_to_cart),0) as add_to_cart
    FROM cleaned_data
    GROUP BY 1,2,3,4,5,6,7,8,9,10
    {% if not loop.last %}UNION ALL
    {% endif %}
{% endfor %}
