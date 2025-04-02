{{ config (
    alias = target.database + '_blended_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
  
WITH initial_s3_data as 
    (SELECT *, {{ get_date_parts('activation_date') }} FROM {{ source('s3_raw','lasttouch_performance') }}  WHERE utm_campaign IS NOT NULL),
  
    s3_data as
    ({%- for date_granularity in date_granularity_list %}    
        SELECT '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
            utm_campaign::varchar, google_campaign, bing_campaign,
            CASE 
                when utm_content = 'prospecting2' then 'Prospecting 2'
                when utm_content = 'prospecting' then 'Prospecting'
                else REPLACE(utm_content, 'DS01_DG_US CAN', 'DS01_DG_US+CAN')
            end as utm_content_adj,
            CASE
                WHEN utm_term = 'Bloating_30_FINECUT_A_Broadcast-Video Image' THEN 'Bloating_30_FINECUT_A_Broadcast'
                WHEN utm_term = 'DD_SEE01_ASMRUnboxing_CantPoop_V1-Video_In-Stream' THEN 'DD_SEE01_ASMRUnboxing_CantPoop_V1_9x16-Video_In-Stream'
                WHEN utm_term = 'DD_SEE01_ASMRUnboxing_CantPoop_V1-Video_Shorts' THEN 'DD_SEE01_ASMRUnboxing_CantPoop_V1_9x16-Video_Shorts'
                WHEN utm_term = 'DD_SEE02_T2CListicle_PoopEveryday_4x5_V4-Video' THEN 'DD_SEE02_T2CListicle_PoopEveryday_4x5_V4-Video_In-Feed'
                WHEN utm_term = 'DD_SEE02_T2CListicle_PoopEveryday_4x5_V4-Video Image' THEN 'DD_SEE02_T2CListicle_PoopEveryday_4x5_V4-Video_In-Feed'
                WHEN utm_term = 'DD-SEE20-YTRecuts-T2CListicle-De-bloatFast-C2-16x9-Video' THEN 'DD_SEE20_YTRecuts_T2CListicle_De-bloatFast_C2_16x9'
                WHEN utm_term = 'DH_Const_UGC_Stomach_Kaleina_Video_39s_DD-Video_In-Stream' THEN 'DH_Const_UGC_Stomach_Kaleina_Video_39s_DD_9x16-Video_In-Stream'
                WHEN utm_term = 'DH_Const_UGC_Stomach_Kaleina_Video_39s_DD-Video_shorts' THEN 'DH_Const_UGC_Stomach_Kaleina_Video_39s_DD_9x16-Video_Shorts'
                WHEN utm_term = 'DH_Poop_UGC_WK_Video_17s_DD_V2_NOW25-Video_In-Stream' THEN 'DH_Poop_UGC_WK_Video_17s_DD_V2_NOW25_4x5_9x16-Video_In-Stream'
                WHEN utm_term = 'DS-01_DemandGenAsset_V2_20250113_1200x628-Video Image' THEN 'DS-01_DemandGenAsset_V2_20250113_1200x628'
                WHEN utm_term = 'DS-01_DemandGenAsset_V3_PoopingEveryDay_20250113_1200x628-Video Image' THEN 'DS-01_DemandGenAsset_V3_PoopingEveryDay_20250113_1200x628'
                WHEN utm_term = 'DS-01_FastSustainedReliefMessagingYoutube_V1-Video Image' THEN 'DS-01_FastSustainedReliefMessagingYoutube_V1-Video+Image'
                WHEN utm_term = 'DS-01_Member_Stat_Hook_V1_20240716_1080x1080-Video' THEN 'DS-01_Member_Stat_Hook_V1_20240716_1080x1080'
                WHEN utm_term = 'DS-01_NewPoopingEverydayTemplateTesting_Static_V3-Video Image' THEN 'DS-01_NewPoopingEverydayTemplateTesting_Static_V3-Video+Image'
                WHEN utm_term = 'DS-01_NewPoopingEverydayTemplateTesting_Static_V4-Video Image' THEN 'DS-01_NewPoopingEverydayTemplateTesting_Static_V4-Video+Image'
                WHEN utm_term = 'DS-01_QualityOfLifeExploration_Static_V1_20240719_1080x1080-Video Image' THEN 'DS-01_QualityOfLifeExploration_Static_V1_20240719_1080x1080-Video+Image'
                WHEN utm_term = 'DS-01_QualityOfLifeExploration_Static_V2_20240719_1080x1080-Video Image' THEN 'DS-01_QualityOfLifeExploration_Static_V2_20240719_1080x1080-Video+Image'
                WHEN utm_term = 'DS-01_SurvivabilityItr_op1_20240327_1x1-Video' THEN 'DS-01_SurvivabilityItr_op1_20240327_1x1'
                WHEN utm_term = 'DS-01_SurvivabilityItr_op1_20240327_1x1-Video Image' THEN 'DS-01_SurvivabilityItr_op1_20240327_1x1'
                WHEN utm_term = 'DS01_KOLYouTubeTesting_V1_NOW25-Video_In-Stream' THEN 'DS01_KOLYouTubeTesting_V1_NOW25_1x1_16x9-Video_In-Stream'
                WHEN utm_term = 'DS01_KOLYouTubeTesting_V1_NOW25-Video_Shorts' THEN 'DS01_KOLYouTubeTesting_V1_NOW25_AllFormats-Video_Shorts'
                WHEN utm_term = 'SEED_REVEAL_30_Fem_02-Video' THEN 'SEED_REVEAL_30_Fem_02'
                ELSE utm_term
            END AS utm_term_adj,
            CASE WHEN channel ~* 'meta' THEN 'Meta' 
                WHEN channel ~* 'google' THEN 'Google Ads' 
                WHEN channel ~* 'youtube' THEN 'Youtube' 
                WHEN channel ~* 'bing' THEN 'Bing' 
            END as channel_adj,
            CASE WHEN utm_campaign ~* 'INTL' THEN 'INTL' ELSE 'US' END as market,
            CASE WHEN utm_campaign ~* 'DS01' THEN 'DS01'
                WHEN utm_campaign ~* 'VS01' THEN 'VS01'
                WHEN utm_campaign ~* 'PDS08' THEN 'PDS08'
                WHEN utm_campaign ~* 'HCP' THEN 'HCP'
                ELSE 'Other'
            END as product,    
            CASE 
                WHEN google_campaign ~* 'amazon' OR bing_campaign ~* 'amazon' THEN 'Amazon'
                WHEN google_campaign ~* 'demand' OR utm_campaign ~* 'demand' THEN 'Demand Gen'
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
            (SELECT CASE WHEN utm_campaign ~* 'demandgen' THEN 'YOUTUBE' ELSE 'GOOGLE' END as channel, campaign_name::varchar as google_campaign, utm_campaign::varchar
            FROM {{ source('gsheet_raw','utm_campaign_list') }} 
            WHERE channel = 'google' AND utm_campaign IS NOT NULL) USING(channel, utm_campaign)
        LEFT JOIN 
            (SELECT 'BING' as channel, campaign_name::varchar as bing_campaign, utm_campaign::varchar
            FROM {{ source('gsheet_raw','utm_campaign_list') }} 
            WHERE channel = 'bing' AND utm_campaign IS NOT NULL) USING(channel, utm_campaign)
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
        SELECT 'Google Ads' as channel, gc.date, gc.date_granularity, country as market, product, 
            CASE WHEN campaign_type_custom = 'Amazon' THEN campaign_name::varchar ELSE google_campaign::varchar END as google_campaign, null as bing_campaign, utm_campaign::varchar, 
            campaign_type_custom as campaign_type, null as utm_content, null as utm_term,
            COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(checkout_initiated),0) as checkout_initiated,
            COALESCE(SUM(add_to_cart),0) as add_to_cart, 0 as leads, COALESCE(SUM(purchases),0) as purchases, 0 as "VS-01 WK", COALESCE(SUM(revenue),0) as revenue, 0 as ft_orders, 0 as lt_orders
        FROM {{ source('reporting','googleads_campaign_performance') }} gc
        LEFT JOIN (SELECT utm_campaign::varchar, google_campaign, COUNT(*) FROM s3_data GROUP BY 1,2) utm ON gc.campaign_name = utm.google_campaign 
        WHERE campaign_type_custom != 'Youtube' and campaign_type_custom != 'Demand Gen'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11
        UNION ALL
        SELECT 'Google Ads' as channel, yt.date, yt.date_granularity, country as market, product, campaign_name::varchar as google_campaign, null as bing_campaign, utm_campaign::varchar, 
            campaign_type_custom as campaign_type, ad_group_name::varchar as utm_content, ad_name::varchar as utm_term,
            COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(checkout_initiated),0) as checkout_initiated,
            COALESCE(SUM(add_to_cart),0) as add_to_cart, 0 as leads, COALESCE(SUM(purchases),0) as purchases, 0 as "VS-01 WK", COALESCE(SUM(revenue),0) as revenue, 0 as ft_orders, 0 as lt_orders
        FROM {{ source('reporting','googleads_ad_performance') }} yt
        LEFT JOIN (SELECT utm_campaign::varchar, google_campaign, COUNT(*) FROM s3_data GROUP BY 1) utm ON yt.campaign_name = utm.google_campaign 
        WHERE (campaign_type_custom = 'Youtube' or campaign_type_custom = 'Demand Gen')
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
        SELECT CASE WHEN channel_adj::varchar = 'Google Ads' OR channel_adj::varchar = 'Youtube' THEN 'Google Ads' ELSE channel_adj::varchar END as channel, date, date_granularity, market, product, 
            google_campaign::varchar, bing_campaign::varchar, utm_campaign::varchar, campaign_type::varchar, 
            CASE WHEN channel_adj = 'Google Ads' OR channel_adj = 'Bing' THEN null ELSE utm_content_adj END as utm_content, 
            CASE WHEN channel_adj = 'Google Ads' OR channel_adj = 'Bing' THEN null ELSE utm_term_adj END as utm_term,
            0 as spend, 0 as impressions, 0 as clicks, 0 as checkout_initiated, 0 as add_to_cart, 0 as leads, 0 as purchases, 0 as "VS-01 WK", 0 as revenue, ft_orders, lt_orders
        FROM s3_data)
    GROUP BY channel, date, date_granularity, market, product, google_campaign, bing_campaign, utm_campaign, campaign_type, utm_content, utm_term)
    
SELECT channel, 
  date, 
  date_granularity, 
  market, 
  product, 
  google_campaign,
  bing_campaign,
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
