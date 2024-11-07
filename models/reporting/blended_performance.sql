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
            utm_campaign::varchar, google_campaign, bing_campaign,
            utm_content, utm_term,
            CASE WHEN channel ~* 'meta' THEN 'Meta' 
                WHEN channel ~* 'google' THEN 'Google Ads' 
                WHEN channel ~* 'youtube' THEN 'Youtube' 
                WHEN channel ~* 'bing' THEN 'Bing' 
            END as channel,
            CASE WHEN utm_campaign ~* 'INTL' THEN 'INTL' ELSE 'US' END as market,
            CASE WHEN utm_campaign ~* 'DS01' THEN 'DS01'
                WHEN utm_campaign ~* 'VS01' THEN 'VS01'
                WHEN utm_campaign ~* 'PDS08' THEN 'PDS08'
                WHEN utm_campaign ~* 'HCP' THEN 'HCP'
                ELSE 'Other'
            END as product,    
            CASE 
                WHEN google_campaign ~* 'amazon' OR bing_campaign ~* 'amazon' THEN 'Amazon'
                WHEN google_campaign ~* 'YT' OR channel ~* 'youtube' THEN 'Youtube'
                WHEN (google_campaign ~* 'Shopping' AND google_campaign ~* 'Brand') OR (bing_campaign ~* 'Shopping' AND bing_campaign ~* 'Brand') THEN 'Shopping - Brand'
                WHEN google_campaign ~* 'Shopping' AND google_campaign !~* 'Brand' THEN 'Shopping - Non Brand'
                WHEN google_campaign ~* 'Performance Max' OR google_campaign ~* 'PMax' THEN 'PMax'
                WHEN google_campaign ~* 'NB' OR bing_campaign ~* 'NB' THEN 'Non Brand'
                WHEN google_campaign ~* 'Brand' OR bing_campaign ~* 'Brand' THEN 'Branded'
                WHEN utm_campaign ~* 'Prospect' OR utm_campaign ~* 'Interest' THEN 'Prospecting'
                WHEN utm_campaign ~* 'Retarget' THEN 'Retargeting'
                ELSE 'Other'
            END as campaign_type,
            COALESCE(SUM(fta_subs),0) as ft_orders, COALESCE(SUM(lta_subs),0) as lt_orders
        FROM initial_s3_data
        LEFT JOIN 
            (SELECT campaign_name::varchar as google_campaign, utm_campaign::varchar
            FROM {{ source('gsheet_raw','utm_campaign_list') }} 
            WHERE channel = 'google') USING(utm_campaign)
        LEFT JOIN 
            (SELECT campaign_name::varchar as bing_campaign, utm_campaign::varchar
            FROM {{ source('gsheet_raw','utm_campaign_list') }} 
            WHERE channel = 'bing') USING(utm_campaign)
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11
        {% if not loop.last %}UNION ALL
        {% endif %}
    {% endfor %}),
  
    final_data as
    (SELECT channel, date::date, date_granularity, market, product, google_campaign, bing_campaign, utm_campaign, campaign_type, utm_content, utm_term,
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(checkout_initiated),0) as checkout_initiated, 
        COALESCE(SUM(add_to_cart),0) as add_to_cart, COALESCE(SUM(leads),0) as leads, COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM("VS-01 WK"),0) as "VS-01 WK",
        COALESCE(SUM(revenue),0) as revenue, COALESCE(SUM(ft_orders),0) as ft_orders, COALESCE(SUM(lt_orders),0) as lt_orders
    FROM
        (SELECT 'Meta' as channel, date, date_granularity, null as market, product, null as google_campaign, null as bing_campaign, campaign_name::varchar as utm_campaign, 
            CASE WHEN campaign_name ~* 'Prospect' OR campaign_name ~* 'Interest' THEN 'Prospecting' WHEN campaign_name ~* 'Retarget' THEN 'Retargeting' END as campaign_type,
            adset_name as utm_content, ad_name as utm_term,
            spend, impressions, link_clicks as clicks, 0 as checkout_initiated, add_to_cart, leads, purchases, "VS-01 WK", 0 as revenue, 0 as ft_orders, 0 as lt_orders
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, gc.date, gc.date_granularity, country as market, product, google_campaign::varchar, null as bing_campaign, utm_campaign::varchar, 
            campaign_type_custom as campaign_type, null as utm_content, null as utm_term,
            COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(checkout_initiated),0) as checkout_initiated,
            0 as add_to_cart, 0 as leads, COALESCE(SUM(purchases),0) as purchases, 0 as "VS-01 WK", COALESCE(SUM(revenue),0) as revenue, 0 as ft_orders, 0 as lt_orders
        FROM {{ source('reporting','googleads_campaign_performance') }} gc
        LEFT JOIN (SELECT utm_campaign::varchar, google_campaign, COUNT(*) FROM s3_data GROUP BY 1,2) utm ON gc.campaign_name = utm.google_campaign 
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11
        UNION ALL
        SELECT 'Bing' as channel, bc.date::date, bc.date_granularity, country::varchar as market, product::varchar, null as google_campaign, bing_campaign::varchar, utm_campaign::varchar, 
            campaign_type_custom::varchar as campaign_type, null as utm_content, null as utm_term,
            COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, 0 as checkout_initiated,
            0 as add_to_cart, 0 as leads, COALESCE(SUM(purchases),0) as purchases, 0 as "VS-01 WK", COALESCE(SUM(revenue),0) as revenue, 0 as ft_orders, 0 as lt_orders
        FROM {{ source('reporting','bingads_campaign_performance') }} bc
        LEFT JOIN (SELECT utm_campaign, bing_campaign, COUNT(*) FROM s3_data GROUP BY 1,2) utm ON bc.campaign_name = utm.bing_campaign 
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11
        UNION ALL
        SELECT CASE WHEN channel::varchar = 'Google Ads' OR channel::varchar = 'Youtube' THEN 'Google Ads' ELSE channel::varchar END as channel, date, date_granularity, market, product, 
            google_campaign::varchar, bing_campaign::varchar, utm_campaign::varchar, campaign_type::varchar, 
            CASE WHEN channel = 'Google Ads' OR channel = 'Bing' THEN null ELSE utm_content END as utm_content, 
            CASE WHEN channel = 'Google Ads' OR channel = 'Bing' THEN null ELSE utm_term END as utm_term,
            0 as spend, 0 as impressions, 0 as clicks, 0 as checkout_initiated, 0 as add_to_cart, 0 as leads, 0 as purchases, 0 as "VS-01 WK", 0 as revenue, ft_orders, lt_orders
        FROM s3_data)
    GROUP BY channel, date, date_granularity, market, product, google_campaign, bing_campaign, utm_campaign, campaign_type, utm_content, utm_term)
    
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
  checkout_initiated,
  leads,
  purchases,
  "VS-01 WK",
  revenue,
  ft_orders,
  lt_orders
FROM final_data
