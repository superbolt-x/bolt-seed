{{ config (
    alias = target.database + '_blended_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
  
WITH last_updated_data as
    (SELECT *, MAX(_fivetran_synced) OVER (PARTITION BY activation_date,channel,utm_campaign,utm_content,utm_term) as last_updated_date
    FROM {{ source('s3_raw','lasttouch_performance') }} 
    WHERE _file ~* 'grouped'),
    
    initial_s3_data as 
    (SELECT *, {{ get_date_parts('activation_date') }} FROM last_updated_data where _fivetran_synced = last_updated_date),
  
    s3_data as
    ({%- for date_granularity in date_granularity_list %}    
        SELECT '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
            utm_campaign::varchar, campaign_name::varchar as google_campaign,
            utm_content, utm_term,
            CASE WHEN channel ~* 'meta' THEN 'Meta' WHEN channel ~* 'google' THEN 'Google Ads' END as channel,
            CASE WHEN utm_campaign ~* 'INTL' THEN 'INTL' ELSE 'US' END as market,
            CASE WHEN utm_campaign ~* 'DS01' THEN 'DS01'
                WHEN utm_campaign ~* 'VS01' THEN 'VS01'
                WHEN utm_campaign ~* 'PDS08' THEN 'PDS08'
                WHEN utm_campaign ~* 'HCP' THEN 'HCP'
                ELSE 'Other'
            END as product,    
            CASE 
                WHEN utm_campaign ~* 'YT' AND utm_campaign !~* 'amazon' THEN 'Youtube'
                WHEN utm_campaign ~* 'Shopping' AND utm_campaign ~* 'Brand' AND utm_campaign !~* 'amazon' THEN 'Shopping - Brand'
                WHEN utm_campaign ~* 'Shopping' AND utm_campaign !~* 'Brand' AND utm_campaign !~* 'amazon' THEN 'Shopping - Non Brand'
                WHEN (utm_campaign ~* 'Performance Max' OR utm_campaign ~* 'PMax') AND utm_campaign !~* 'amazon' THEN 'PMax'
                WHEN utm_campaign ~* 'NB' AND utm_campaign !~* 'amazon' THEN 'Non Brand'
                WHEN utm_campaign ~* 'Brand' AND utm_campaign !~* 'amazon' THEN 'Branded'
                WHEN campaign_name ~* 'Prospect' OR campaign_name ~* 'Interest' THEN 'Prospecting'
                WHEN campaign_name ~* 'Retarget' THEN 'Retargeting'
                ELSE 'Other'
            END as campaign_type,
            COALESCE(SUM(fta_subs),0) as ft_orders, COALESCE(SUM(lta_subs),0) as lt_orders
        FROM initial_s3_data
        LEFT JOIN (SELECT utm_campaign::varchar, campaign_name, COUNT(*) FROM {{ source('gsheet_raw','utm_campaign_list') }} GROUP BY 1,2) USING(utm_campaign)
        GROUP BY 1,2,3,4,5,6,7,8
        {% if not loop.last %}UNION ALL
        {% endif %}
    {% endfor %}),
  
    final_data as
    (SELECT channel, date::date, date_granularity, market, product, google_campaign, utm_campaign, campaign_type, utm_content, utm_term,
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(add_to_cart),0) as add_to_cart, COALESCE(SUM(leads),0) as leads, COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM("VS-01 WK"),0) as "VS-01 WK",
        COALESCE(SUM(revenue),0) as revenue, COALESCE(SUM(ft_orders),0) as ft_orders, COALESCE(SUM(lt_orders),0) as lt_orders
    FROM
        (SELECT 'Meta' as channel, date, date_granularity, null as market, product, null as google_campaign, campaign_name as utm_campaign, 
            CASE WHEN campaign_name ~* 'Prospect' OR campaign_name ~* 'Interest' THEN 'Prospecting' WHEN campaign_name ~* 'Retarget' THEN 'Retargeting' END as campaign_type,
            adset_name as utm_content, ad_name as utm_term,
            spend, impressions, link_clicks as clicks, add_to_cart, leads, purchases, "VS-01 WK", 0 as revenue, 0 as ft_orders, 0 as lt_orders
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, gc.date, gc.date_granularity, country as market, product, campaign_name::varchar as google_campaign, utm_campaign, campaign_type_custom as campaign_type, 
            null as utm_content, null as utm_term,
            COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks,
            0 as add_to_cart, 0 as leads, COALESCE(SUM(purchases),0) as purchases, 0 as "VS-01 WK", COALESCE(SUM(revenue),0) as revenue, 0 as ft_orders, 0 as lt_orders
        FROM {{ source('reporting','googleads_campaign_performance') }} gc
        LEFT JOIN (SELECT utm_campaign, google_campaign, COUNT(*) FROM s3_data GROUP BY 1,2) utm ON gc.campaign_name = utm.google_campaign 
        WHERE campaign_type_custom NOT IN ('Non Brand','Branded')
        GROUP BY 1,2,3,4,5,6,7,8,9,10
        UNION ALL
        SELECT 'Google Ads' as channel, gck.date, gck.date_granularity, country as market, product, campaign_name::varchar as google_campaign, utm_campaign, campaign_type_custom as campaign_type, 
            null as utm_content, keyword as utm_term,
            COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks,
            0 as add_to_cart, 0 as leads, COALESCE(SUM(purchases),0) as purchases, 0 as "VS-01 WK", COALESCE(SUM(revenue),0) as revenue, 0 as ft_orders, 0 as lt_orders
        FROM {{ source('reporting','googleads_keyword_performance') }} gck
        LEFT JOIN (SELECT utm_campaign, google_campaign, COUNT(*) FROM s3_data GROUP BY 1,2) utm ON gck.campaign_name = utm.google_campaign 
        GROUP BY 1,2,3,4,5,6,7,8,9,10
        UNION ALL
        SELECT channel, date, date_granularity, market, product, google_campaign, utm_campaign, campaign_type, utm_content, 
            CASE WHEN channel = 'Google Ads' AND campaign_type NOT IN ('Non Brand','Branded') THEN null ELSE utm_term END as utm_term,
            0 as spend, 0 as impressions, 0 as clicks, 0 as add_to_cart, 0 as leads, 0 as purchases, 0 as "VS-01 WK", 0 as revenue, ft_orders, lt_orders
        FROM s3_data)
    GROUP BY channel, date, date_granularity, market, product, google_campaign, utm_campaign, campaign_type, utm_content, utm_term)
    
SELECT channel, 
  date, 
  date_granularity, 
  market, 
  product, 
  google_campaign,
  utm_campaign, 
  campaign_type, 
  utm_content, 
  utm_term,
  spend,
  impressions,
  clicks,
  add_to_cart,
  leads,
  purchases,
  "VS-01 WK",
  revenue,
  ft_orders,
  lt_orders
FROM final_data
