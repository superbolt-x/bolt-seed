{{ config (
    alias = target.database + '_facebook_influencer_performance'
)}}

WITH gut_health as
    (SELECT date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, 'Gut Health' as theme,
        CASE WHEN ad_name ~* 'Shred' OR ad_name ~* 'Bobby' THEN 'Male'
            WHEN ad_name ~* 'Shred' AND ad_name !~* 'Bobby' THEN 'Female'
        END as influencer_gender,
        COALESCE(SUM(spend),0) as gh_spend,
        COALESCE(SUM(impressions),0) as gh_impressions,
        COALESCE(SUM(inline_link_clicks),0) as gh_link_clicks,
        COALESCE(SUM(purchases),0) as gh_purchases
    FROM {{ source('facebook_raw','ads_insights_age_gender') }}
    LEFT JOIN 
        (SELECT date, ad_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
        FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2) USING (date,ad_id)
    WHERE ad_name ~* 'INF'
    AND ad_name ~* '_GH_'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11),
    
gut_health_no_bobby as
    (SELECT date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, 'Gut Health + no Bobby Parrish ads' as theme,
        CASE WHEN ad_name ~* 'Shred' OR ad_name ~* 'Bobby' THEN 'Male'
            WHEN ad_name ~* 'Shred' AND ad_name !~* 'Bobby' THEN 'Female'
        END as influencer_gender,
        COALESCE(SUM(spend),0) as ghnb_spend,
        COALESCE(SUM(impressions),0) as ghnb_impressions,
        COALESCE(SUM(inline_link_clicks),0) as ghnb_link_clicks,
        COALESCE(SUM(purchases),0) as ghnb_purchases
    FROM {{ source('facebook_raw','ads_insights_age_gender') }}
    LEFT JOIN 
        (SELECT date, ad_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
        FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2) USING (date,ad_id)
    WHERE ad_name ~* 'INF'
    AND (ad_name ~* '_GH_' AND ad_name !~* 'Bobby')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11),
    
immune_health as
    (SELECT date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, 'Immune Health' as theme,
        CASE WHEN ad_name ~* 'Shred' OR ad_name ~* 'Bobby' THEN 'Male'
            WHEN ad_name ~* 'Shred' AND ad_name !~* 'Bobby' THEN 'Female'
        END as influencer_gender,
        COALESCE(SUM(spend),0) as ih_spend,
        COALESCE(SUM(impressions),0) as ih_impressions,
        COALESCE(SUM(inline_link_clicks),0) as ih_link_clicks,
        COALESCE(SUM(purchases),0) as ih_purchases
    FROM {{ source('facebook_raw','ads_insights_age_gender') }}
    LEFT JOIN 
        (SELECT date, ad_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
        FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2) USING (date,ad_id)
    WHERE ad_name ~* 'INF'
    AND ad_name ~* '_IH_'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11),

product_features as 
    (SELECT date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, 'Product Features' as theme,
        CASE WHEN ad_name ~* 'Shred' OR ad_name ~* 'Bobby' THEN 'Male'
            WHEN ad_name ~* 'Shred' AND ad_name !~* 'Bobby' THEN 'Female'
        END as influencer_gender,
        COALESCE(SUM(spend),0) as pf_spend,
        COALESCE(SUM(impressions),0) as pf_impressions,
        COALESCE(SUM(inline_link_clicks),0) as pf_link_clicks,
        COALESCE(SUM(purchases),0) as pf_purchases
    FROM {{ source('facebook_raw','ads_insights_age_gender') }}
    LEFT JOIN 
        (SELECT date, ad_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
        FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2) USING (date,ad_id)
    WHERE ad_name ~* 'INF'
    AND ad_name ~* '_PF_'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11),

total_body_health as
    (SELECT date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, 'Total Body Health' as theme,
        CASE WHEN ad_name ~* 'Shred' OR ad_name ~* 'Bobby' THEN 'Male'
            WHEN ad_name ~* 'Shred' AND ad_name !~* 'Bobby' THEN 'Female'
        END as influencer_gender,
        COALESCE(SUM(spend),0) as tbh_spend,
        COALESCE(SUM(impressions),0) as tbh_impressions,
        COALESCE(SUM(inline_link_clicks),0) as tbh_link_clicks,
        COALESCE(SUM(purchases),0) as tbh_purchases
    FROM {{ source('facebook_raw','ads_insights_age_gender') }}
    LEFT JOIN 
        (SELECT date, ad_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
        FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2) USING (date,ad_id)
    WHERE ad_name ~* 'INF'
    AND ad_name ~* '_TBH_'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11),

total_body_health_no_shred as
    (SELECT date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, 'Total Body Health + no Shred ads' as theme,
        CASE WHEN ad_name ~* 'Shred' OR ad_name ~* 'Bobby' THEN 'Male'
            WHEN ad_name ~* 'Shred' AND ad_name !~* 'Bobby' THEN 'Female'
        END as influencer_gender,
        COALESCE(SUM(spend),0) as tbhns_spend,
        COALESCE(SUM(impressions),0) as tbhns_impressions,
        COALESCE(SUM(inline_link_clicks),0) as tbhns_link_clicks,
        COALESCE(SUM(purchases),0) as tbhns_purchases
    FROM {{ source('facebook_raw','ads_insights_age_gender') }}
    LEFT JOIN 
        (SELECT date, ad_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
        FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2) USING (date,ad_id)
    WHERE ad_name ~* 'INF'
    AND (ad_name ~* '_TBH_' AND ad_name !~* 'Shred')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11),

product_benefits as 
    (SELECT date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, 'Product Benefits' as theme,
        CASE WHEN ad_name ~* 'Shred' OR ad_name ~* 'Bobby' THEN 'Male'
            WHEN ad_name ~* 'Shred' AND ad_name !~* 'Bobby' THEN 'Female'
        END as influencer_gender,
        COALESCE(SUM(spend),0) as pb_spend,
        COALESCE(SUM(impressions),0) as pb_impressions,
        COALESCE(SUM(inline_link_clicks),0) as pb_link_clicks,
        COALESCE(SUM(purchases),0) as pb_purchases
    FROM {{ source('facebook_raw','ads_insights_age_gender') }}
    LEFT JOIN 
        (SELECT date, ad_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
        FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2) USING (date,ad_id)
    WHERE ad_name ~* 'INF'
    AND ad_name ~* '_PB_'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11),

science as
    (SELECT date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, 'Science' as theme,
        CASE WHEN ad_name ~* 'Shred' OR ad_name ~* 'Bobby' THEN 'Male'
            WHEN ad_name ~* 'Shred' AND ad_name !~* 'Bobby' THEN 'Female'
        END as influencer_gender,
        COALESCE(SUM(spend),0) as s_spend,
        COALESCE(SUM(impressions),0) as s_impressions,
        COALESCE(SUM(inline_link_clicks),0) as s_link_clicks,
        COALESCE(SUM(purchases),0) as s_purchases
    FROM {{ source('facebook_raw','ads_insights_age_gender') }}
    LEFT JOIN 
        (SELECT date, ad_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
        FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2) USING (date,ad_id)
    WHERE ad_name ~* 'INF'
    AND ad_name ~* '_SC_'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11),

others as
    (SELECT date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, 'Others' as theme,
        CASE WHEN ad_name ~* 'Shred' OR ad_name ~* 'Bobby' THEN 'Male'
            WHEN ad_name ~* 'Shred' AND ad_name !~* 'Bobby' THEN 'Female'
        END as influencer_gender,
        COALESCE(SUM(spend),0) as o_spend,
        COALESCE(SUM(impressions),0) as o_impressions,
        COALESCE(SUM(inline_link_clicks),0) as o_link_clicks,
        COALESCE(SUM(purchases),0) as o_purchases
    FROM {{ source('facebook_raw','ads_insights_age_gender') }}
    LEFT JOIN 
        (SELECT date, ad_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
        FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2) USING (date,ad_id)
    WHERE ad_name ~* 'INF'
    AND (ad_name !~* '_GH_' AND ad_name !~* '_IH_' AND ad_name !~* '_PF_' AND ad_name !~* '_TBH_' AND ad_name !~* '_PB_' AND ad_name !~* '_SC_')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

SELECT date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, theme, influencer_gender,
    SUM(gh_spend)+SUM(ghnb_spend)+SUM(ih_spend)+SUM(pf_spend)+SUM(tbh_spend)+SUM(tbhns_spend)+SUM(pb_spend)+SUM(s_spend)+SUM(o_spend) as spend,
    SUM(gh_impressions)+SUM(ghnb_impressions)+SUM(ih_impressions)+SUM(pf_impressions)+SUM(tbh_impressions)+SUM(tbhns_impressions)+SUM(pb_impressions)+SUM(s_impressions)+SUM(o_impressions) as impressions,
    SUM(gh_link_clicks)+SUM(ghnb_link_clicks)+SUM(ih_link_clicks)+SUM(pf_link_clicks)+SUM(tbh_link_clicks)+SUM(tbhns_link_clicks)+SUM(pb_link_clicks)+SUM(s_link_clicks)+SUM(o_link_clicks) as link_clicks,
    SUM(gh_purchases)+SUM(ghnb_purchases)+SUM(ih_purchases)+SUM(pf_purchases)+SUM(tbh_purchases)+SUM(tbhns_purchases)+SUM(pb_purchases)+SUM(s_purchases)+SUM(o_purchases) as purchases
FROM gut_health 
LEFT JOIN gut_health_no_bobby USING(date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, theme, influencer_gender)
LEFT JOIN immune_health USING(date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, theme, influencer_gender)
LEFT JOIN product_features USING(date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, theme, influencer_gender)
LEFT JOIN total_body_health USING(date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, theme, influencer_gender)
LEFT JOIN total_body_health_no_shred USING(date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, theme, influencer_gender)
LEFT JOIN product_benefits USING(date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, theme, influencer_gender)
LEFT JOIN science USING(date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, theme, influencer_gender)
LEFT JOIN others USING(date, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, age, gender, theme, influencer_gender)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
