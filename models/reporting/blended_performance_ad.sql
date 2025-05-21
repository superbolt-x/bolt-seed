{{ config (
    alias = target.database + '_blended_performance_ad'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
  
WITH initial_s3_data as 
    (SELECT *, {{ get_date_parts('activation_date') }} FROM {{ source('s3_raw','lasttouch_performance') }}  WHERE utm_campaign IS NOT NULL),
  
    s3_data as
    ({%- for date_granularity in date_granularity_list %}    
        SELECT '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
            utm_campaign::varchar, google_campaign,
            CASE 
                when utm_content = 'prospecting2' then 'Prospecting 2'
                when utm_content = 'prospecting' then 'Prospecting'
                when utm_content = 'infeed' then 'In-Feed'
                when utm_content = 'instream' then 'In-Stream'
                else REPLACE(utm_content, 'DS01_DG_US CAN', 'DS01_DG_US+CAN')
            end as utm_content_adj,
            CASE 
                WHEN utm_term ~* '5.1.25_DS-01_Testimonial_Bloating_Short_V1_9x16' THEN '5.1.25_DS_01_Testimonial_Bloating_Short_V1_9x16'
                WHEN utm_term ~* 'Bloating_30_FINECUT_A_Broadcast' THEN 'Bloating_30_FINECUT_A_Broadcast'
                WHEN utm_term ~* 'DD_SEE01_ASMRUnboxing_CantPoop_V1_9x16-Video' THEN 'DD_SEE01_ASMRUnboxing_CantPoop_V1_9x16-Video'
                WHEN utm_term ~* 'DD_SEE02_T2CListicle_PoopEveryday_V4_4x5-Video' THEN 'DD_SEE02_T2CListicle_PoopEveryday_V4_4x5-Video'
                WHEN utm_term ~* 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16_4x5_16x9' THEN 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16_4x5_16x9'
                WHEN utm_term ~* 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16_4x5_16x9-Video' THEN 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16_4x5_16x9-Video'
                WHEN utm_term ~* 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16-Video' THEN 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16-Video'
                WHEN utm_term ~* 'DD_SEE20_YTRecuts_T2CListicle_De-bloatFast_C2_9x16_16x9' THEN 'DD_SEE20_YTRecuts_T2CListicle_De_bloatFast_C2_9x16_16x9'
                WHEN utm_term ~* 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1-Survive_SmileyFace_V1_4x5-Video' THEN 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1_Survive_SmileyFace_V1_4x5-Video'
                WHEN utm_term ~* 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1-Survive_SmileyFace_V1_9x16_4x5' THEN 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1_Survive_SmileyFace_V1_9x16_4x5'
                WHEN utm_term ~* 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1-Survive_SmileyFace_V1_9x16_4x5-Video' THEN 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1_Survive_SmileyFace_V1_9x16_4x5-Video'
                WHEN utm_term ~* 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1-Survive_SmileyFace_V1_9x16-Video' THEN 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1_Survive_SmileyFace_V1_9x16-Video'
                WHEN utm_term ~* 'DD_SEE33_ClinicalStudyExplainer_C4_ProblemSolution_Hook2_ALTFONT_V1_4x5-Video' THEN 'DD_SEE33_ClinicalStudyExplainer_C4_ProblemSolution_Hook2_ALTFONT_V1_4x5-Video'
                WHEN utm_term ~* 'DH_2in1_Lifestyle_Video_HQ_42s_9x16-Video' THEN 'DH_2in1_Lifestyle_Video_HQ_42s_9x16-Video'
                WHEN utm_term ~* 'DH_Bloat_Lifestyle_Video_HQ_40s_9x16-Video' THEN 'DH_Bloat_Lifestyle_Video_HQ_40s_9x16-Video'
                WHEN utm_term ~* 'DH_Bloat_UGC_Video_58s_DD_NOW25_9x16-Video' THEN 'DH_Bloat_UGC_Video_58s_DD_NOW25_9x16-Video'
                WHEN utm_term ~* 'DH_Const_UGC_Stomach_Kaleina_Video_39s_DD' THEN 'DH_Const_UGC_Stomach_Kaleina_Video_39s_DD'
                WHEN utm_term ~* 'DH_Const_UGC_Stomach_Kaleina_Video_39s_DD_9x16-Video' THEN 'DH_Const_UGC_Stomach_Kaleina_Video_39s_DD_9x16-Video'
                WHEN utm_term ~* 'DH_PB_Value_WK_Static_HQ_V1' THEN 'DH_PB_Value_WK_Static_HQ_V1'
                WHEN utm_term ~* 'DH_Poop_Lifestyle_Video_DD_42s_9x16-Video' THEN 'DH_Poop_Lifestyle_Video_DD_42s_9x16-Video'
                WHEN utm_term ~* 'DH_Poop_UGC_WK_Video_17s_DD_V2_NOW25_4x5_9x16-Video' THEN 'DH_Poop_UGC_WK_Video_17s_DD_V2_NOW25_4x5_9x16-Video'
                WHEN utm_term ~* 'DS-01_DemandGenAsset_V1_PoopingEveryDay_20250113' THEN 'DS_01_DemandGenAsset_V1_PoopingEveryDay_20250113'
                WHEN utm_term ~* 'DS-01_DemandGenAsset_V2_20250113_1200x628' THEN 'DS_01_DemandGenAsset_V2_20250113_1200x628'
                WHEN utm_term ~* 'DS-01_DemandGenAsset_V3_PoopingEveryDay' THEN 'DS_01_DemandGenAsset_V3_PoopingEveryDay'
                WHEN utm_term ~* 'DS-01_DemandGenAsset_V3_PoopingEveryDay_20250113_1200x628' THEN 'DS_01_DemandGenAsset_V3_PoopingEveryDay_20250113_1200x628'
                WHEN utm_term ~* 'DS-01_FastSustainedReliefMessagingYoutube_V1-Video' THEN 'DS_01_FastSustainedReliefMessagingYoutube_V1-Video'
                WHEN utm_term ~* 'DS-01_FastSustainedReliefMessagingYoutube_V2-Video' THEN 'DS_01_FastSustainedReliefMessagingYoutube_V2-Video'
                WHEN utm_term ~* 'DS-01_GraphicPromoExp_Static2a_NOW25-Video' THEN 'DS_01_GraphicPromoExp_Static2a_NOW25-Video'
                WHEN utm_term ~* 'DS-01_Member_Stat_Hook_V1_1x1-Video' THEN 'DS_01_Member_Stat_Hook_V1_1x1-Video'
                WHEN utm_term ~* 'DS-01_Member_Stat_Hook_V1_20240716_1080x1080' THEN 'DS_01_Member_Stat_Hook_V1_20240716_1080x1080'
                WHEN utm_term ~* 'DS-01_Member_Stat_Hook_V1_9x16_1x1-Video' THEN 'DS_01_Member_Stat_Hook_V1_9x16_1x1-Video'
                WHEN utm_term ~* 'DS-01_Member_Stat_Hook_V1_9x16-Video' THEN 'DS_01_Member_Stat_Hook_V1_9x16-Video'
                WHEN utm_term ~* 'DS-01_NewPoopingEverydayTemplateTesting_Static_V3-Video' THEN 'DS_01_NewPoopingEverydayTemplateTesting_Static_V3-Video'
                WHEN utm_term ~* 'DS-01_NewPoopingEverydayTemplateTesting_Static_V4' THEN 'DS_01_NewPoopingEverydayTemplateTesting_Static_V4'
                WHEN utm_term ~* 'DS-01_NewPoopingEverydayTemplateTesting_Static_V4-Video' THEN 'DS_01_NewPoopingEverydayTemplateTesting_Static_V4-Video'
                WHEN utm_term ~* 'DS-01_Q524TopPerformerRefreshes_V2_Capsule_NOW25-Video' THEN 'DS_01_Q524TopPerformerRefreshes_V2_Capsule_NOW25-Video'
                WHEN utm_term ~* 'DS-01_Q524TopPerformerRefreshes_V3_Capsule_Opt2_NOW25-Video' THEN 'DS_01_Q524TopPerformerRefreshes_V3_Capsule_Opt2_NOW25-Video'
                WHEN utm_term ~* 'DS-01_QualityOfLifeExploration_Static_V1_20240719_1080x1080-Video' THEN 'DS_01_QualityOfLifeExploration_Static_V1_20240719_1080x1080-Video'
                WHEN utm_term ~* 'DS-01_QualityOfLifeExploration_Static_V2_20240719_1080x1080-Video' THEN 'DS_01_QualityOfLifeExploration_Static_V2_20240719_1080x1080-Video'
                WHEN utm_term ~* 'DS-01_SurvivabilityItr_op1_20240327_1x1' THEN 'DS_01_SurvivabilityItr_op1_20240327_1x1'
                WHEN utm_term ~* 'DS-01_Top_Quote_Light_Motion_V1_20240625_1080x1920' THEN 'DS_01_Top_Quote_Light_Motion_V1_20240625_1080x1920'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V1_1x1_16x9-Video' THEN 'DS01_KOLYouTubeTesting_V1_1x1_16x9-Video'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V1_9x16_1x1_16x9-Video' THEN 'DS01_KOLYouTubeTesting_V1_9x16_1x1_16x9-Video'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V1_9x16-Video' THEN 'DS01_KOLYouTubeTesting_V1_9x16-Video'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V1_NOW25_1x1_16x9-Video' THEN 'DS01_KOLYouTubeTesting_V1_NOW25_1x1_16x9-Video'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V1_NOW25_AllFormats-Video' THEN 'DS01_KOLYouTubeTesting_V1_NOW25_AllFormats-Video'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V2_KaitDavis_AllFormats-Video' THEN 'DS01_KOLYouTubeTesting_V2_KaitDavis_AllFormats-Video'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V2_KaitDavis_NOW25_AllFormats-Video' THEN 'DS01_KOLYouTubeTesting_V2_KaitDavis_NOW25_AllFormats-Video'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V3_1080x1080-Video' THEN 'DS01_KOLYouTubeTesting_V3_1080x1080-Video'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V3_16x9_1x1-Video' THEN 'DS01_KOLYouTubeTesting_V3_16x9_1x1-Video'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V3_9x16_1x1_16x9-Video' THEN 'DS01_KOLYouTubeTesting_V3_9x16_1x1_16x9-Video'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V3_9x16-Video' THEN 'DS01_KOLYouTubeTesting_V3_9x16-Video'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V1_NOW25-Video_In-Stream' THEN 'DS01_KOLYouTubeTesting_V1_NOW25-Video_InStream'
                WHEN utm_term ~* 'GH_2in1_Lifestyle_Video_DD_40s_9x16-Video' THEN 'GH_2in1_Lifestyle_Video_DD_40s_9x16-Video'
                WHEN utm_term ~* 'GH_UGC_HopewellHeights_Video_63s_INF_9x16-Video' THEN 'GH_UGC_HopewellHeights_Video_63s_INF_9x16-Video'
                WHEN utm_term ~* 'SEED_REVEAL_30_Fem_02' THEN 'SEED_REVEAL_30_Fem_02'
                WHEN utm_term ~* 'DD-SEE20-YTRecuts-T2CListicle-De-bloatFast-C2-16x9' THEN 'DD_SEE20_YTRecuts_T2CListicle_De_bloatFast_C2_16x9'
                WHEN utm_term ~* 'DD_SEE20-YTRecuts-T2CListicle-De-bloatFast-C2-16x9' THEN 'DD_SEE20_YTRecuts_T2CListicle_De_bloatFast_C2_16x9'
                WHEN utm_term ~* 'DD_SEE01_ASMRUnboxing_CantPoop_V1' THEN 'DD_SEE01_ASMRUnboxing_CantPoop_V1_9x16-Video'
                WHEN utm_term ~* 'DD_SEE01_ASMRUnboxing_CantPoop_V1' THEN 'DD_SEE01_ASMRUnboxing_CantPoop_V1_9x16-Video'
                WHEN utm_term ~* 'DD_SEE02_T2CListicle_PoopEveryday_4x5_V4' THEN 'DD_SEE02_T2CListicle_PoopEveryday_V4_4x5-Video'
                WHEN utm_term ~* 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16' THEN 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16-Video'
                WHEN utm_term ~* 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1-Survive_SmileyFace_DS-01_4x5_V1' THEN 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1_Survive_SmileyFace_V1_4x5-Video'
                WHEN utm_term ~* 'DH_2in1_Lifestyle_Video_HQ_42s_9x16' THEN 'DH_2in1_Lifestyle_Video_HQ_42s_9x16-Video'
                WHEN utm_term ~* '4.25.25_DH_Bloat_Claim_Jar_Static_HQ_V2' THEN '4.25.25_DH_Bloat_Claim_Jar_Static_HQ_V2'
                WHEN utm_term ~* 'DH_Bloat_Claim_Jar_Static_HQ' THEN 'DH_Bloat_Claim_Jar_Static_HQ'
                WHEN utm_term ~* 'DH_Gas_Claim_Capsule_Static_HQ' THEN 'DH_Gas_Claim_Capsule_Static_HQ'
                WHEN utm_term ~* 'DH_Poop_UGC_WK_Video_17s_DD_V2_NOW25' THEN 'DH_Poop_UGC_WK_Video_17s_DD_V2_NOW25_4x5_9x16-Video'
                WHEN utm_term ~* 'DS-01-DemandGenAsset-V1-PoopingEveryDay_20250113' THEN 'DS_01_DemandGenAsset_V1_PoopingEveryDay_20250113'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V1_1080x1080' THEN 'DS01_KOLYouTubeTesting_V1_1x1_16x9-Video'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V1_NOW25_1x1_16x9-Video_In-Stream' THEN 'DS01_KOLYouTubeTesting_V1_NOW25_1x1_16x9-Video_InStream'
                WHEN utm_term ~* 'DS01_KOLYouTubeTesting_V1_NOW25_AllFormats-Video_Shorts' THEN 'DS01_KOLYouTubeTesting_V1_NOW25_AllFormats-Video_Shorts'
                WHEN utm_term ~* 'DH_PB_Claim_Jar_Static_HQ-Video' THEN 'DH_PB_Claim_Jar_Static_HQ-Video'
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
                WHEN google_campaign ~* 'amazon' THEN 'Amazon'
                WHEN google_campaign ~* 'demand' OR utm_campaign ~* 'demand' THEN 'Demand Gen'
                WHEN google_campaign ~* 'YT' OR channel ~* 'youtube' THEN 'Youtube'
                WHEN (google_campaign ~* 'Shopping' AND google_campaign ~* 'Brand') THEN 'Shopping - Brand'
                WHEN google_campaign ~* 'Shopping' AND google_campaign !~* 'Brand' THEN 'Shopping - Non Brand'
                WHEN google_campaign ~* 'Performance Max' OR google_campaign ~* 'PMax' THEN 'PMax'
                WHEN google_campaign ~* 'NB' THEN 'Non Brand'
                WHEN google_campaign ~* 'Brand' THEN 'Branded'
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
        WHERE (channel ~* 'google' or channel ~* 'youtube')
        GROUP BY 1,2,3,4,5,6,7,8,9,10
        {% if not loop.last %}UNION ALL
        {% endif %}
    {% endfor %}),

    platform_data as (
    SELECT 'Google Ads' as channel, yt.date, yt.date_granularity, country as market, product, campaign_name::varchar as google_campaign, utm_campaign::varchar, 
            campaign_type_custom as campaign_type, ad_group_name::varchar as utm_content,  
            CASE 
                WHEN ad_name ~* '5.1.25_DS-01_Testimonial_Bloating_Short_V1_9x16' THEN '5.1.25_DS_01_Testimonial_Bloating_Short_V1_9x16'
                WHEN ad_name ~* 'Bloating_30_FINECUT_A_Broadcast' THEN 'Bloating_30_FINECUT_A_Broadcast'
                WHEN ad_name ~* 'DD_SEE01_ASMRUnboxing_CantPoop_V1_9x16-Video' THEN 'DD_SEE01_ASMRUnboxing_CantPoop_V1_9x16-Video'
                WHEN ad_name ~* 'DD_SEE02_T2CListicle_PoopEveryday_V4_4x5-Video' THEN 'DD_SEE02_T2CListicle_PoopEveryday_V4_4x5-Video'
                WHEN ad_name ~* 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16_4x5_16x9' THEN 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16-Video'
                WHEN ad_name ~* 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16_4x5_16x9-Video' THEN 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16-Video'
                WHEN ad_name ~* 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16-Video' THEN 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16-Video'
                WHEN ad_name ~* 'DD_SEE20_YTRecuts_T2CListicle_De-bloatFast_C2_9x16_16x9' THEN 'DD_SEE20_YTRecuts_T2CListicle_De_bloatFast_C2_16x9'
                WHEN ad_name ~* 'DD_SEE20-YTRecuts_T2CListicle_De-bloatFast_C2_9x16_16x9' THEN 'DD_SEE20_YTRecuts_T2CListicle_De_bloatFast_C2_16x9'
                WHEN ad_name ~* 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1-Survive_SmileyFace_V1_4x5-Video' THEN 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1_Survive_SmileyFace_V1_4x5-Video'
                WHEN ad_name ~* 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1-Survive_SmileyFace_V1_9x16_4x5' THEN 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1_Survive_SmileyFace_V1_9x16_4x5'
                WHEN ad_name ~* 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1-Survive_SmileyFace_V1_9x16_4x5-Video' THEN 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1_Survive_SmileyFace_V1_9x16_4x5-Video'
                WHEN ad_name ~* 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1-Survive_SmileyFace_V1_9x16-Video' THEN 'DD_SEE24_WhiteboardExplainer_C4I_Explainer_Hook1_Survive_SmileyFace_V1_9x16-Video'
                WHEN ad_name ~* 'DD_SEE33_ClinicalStudyExplainer_C4_ProblemSolution_Hook2_ALTFONT_V1_4x5-Video' THEN 'DD_SEE33_ClinicalStudyExplainer_C4_ProblemSolution_Hook2_ALTFONT_V1_4x5-Video'
                WHEN ad_name ~* 'DH_2in1_Lifestyle_Video_HQ_42s_9x16-Video' THEN 'DH_2in1_Lifestyle_Video_HQ_42s_9x16-Video'
                WHEN ad_name ~* 'DH_Bloat_Lifestyle_Video_HQ_40s_9x16-Video' THEN 'DH_Bloat_Lifestyle_Video_HQ_40s_9x16-Video'
                WHEN ad_name ~* 'DH_Bloat_UGC_Video_58s_DD_NOW25_9x16-Video' THEN 'DH_Bloat_UGC_Video_58s_DD_NOW25_9x16-Video'
                WHEN ad_name ~* 'DH_Const_UGC_Stomach_Kaleina_Video_39s_DD' THEN 'DH_Const_UGC_Stomach_Kaleina_Video_39s_DD'
                WHEN ad_name ~* 'DH_Const_UGC_Stomach_Kaleina_Video_39s_DD_9x16-Video' THEN 'DH_Const_UGC_Stomach_Kaleina_Video_39s_DD_9x16-Video'
                WHEN ad_name ~* 'DH_PB_Value_WK_Static_HQ_V1' THEN 'DH_PB_Value_WK_Static_HQ_V1'
                WHEN ad_name ~* 'DH_Poop_Lifestyle_Video_DD_42s_9x16-Video' THEN 'DH_Poop_Lifestyle_Video_DD_42s_9x16-Video'
                WHEN ad_name ~* 'DH_Poop_UGC_WK_Video_17s_DD_V2_NOW25_4x5_9x16-Video' THEN 'DH_Poop_UGC_WK_Video_17s_DD_V2_NOW25_4x5_9x16-Video'
                WHEN ad_name ~* 'DS-01_DemandGenAsset_V1_PoopingEveryDay_20250113' THEN 'DS_01_DemandGenAsset_V1_PoopingEveryDay_20250113'
                WHEN ad_name ~* 'DS-01_DemandGenAsset_V2_20250113_1200x628' THEN 'DS_01_DemandGenAsset_V2_20250113_1200x628'
                WHEN ad_name ~* 'DS-01_DemandGenAsset_V3_PoopingEveryDay' THEN 'DS_01_DemandGenAsset_V3_PoopingEveryDay'
                WHEN ad_name ~* 'DS-01_DemandGenAsset_V3_PoopingEveryDay_20250113_1200x628' THEN 'DS_01_DemandGenAsset_V3_PoopingEveryDay_20250113_1200x628'
                WHEN ad_name ~* 'DS-01_FastSustainedReliefMessagingYoutube_V1-Video' THEN 'DS_01_FastSustainedReliefMessagingYoutube_V1-Video'
                WHEN ad_name ~* 'DS-01_FastSustainedReliefMessagingYoutube_V2-Video' THEN 'DS_01_FastSustainedReliefMessagingYoutube_V2-Video'
                WHEN ad_name ~* 'DS-01_GraphicPromoExp_Static2a_NOW25-Video' THEN 'DS_01_GraphicPromoExp_Static2a_NOW25-Video'
                WHEN ad_name ~* 'DS-01_Member_Stat_Hook_V1_1x1-Video' THEN 'DS_01_Member_Stat_Hook_V1_1x1-Video'
                WHEN ad_name ~* 'DS-01_Member_Stat_Hook_V1_20240716_1080x1080' THEN 'DS_01_Member_Stat_Hook_V1_20240716_1080x1080'
                WHEN ad_name ~* 'DS-01_Member_Stat_Hook_V1_9x16_1x1-Video' THEN 'DS_01_Member_Stat_Hook_V1_9x16_1x1-Video'
                WHEN ad_name ~* 'DS-01_Member_Stat_Hook_V1_9x16-Video' THEN 'DS_01_Member_Stat_Hook_V1_9x16-Video'
                WHEN ad_name ~* 'DS-01_NewPoopingEverydayTemplateTesting_Static_V3-Video' THEN 'DS_01_NewPoopingEverydayTemplateTesting_Static_V3-Video'
                WHEN ad_name ~* 'DS-01_NewPoopingEverydayTemplateTesting_Static_V4' THEN 'DS_01_NewPoopingEverydayTemplateTesting_Static_V4'
                WHEN ad_name ~* 'DS-01_NewPoopingEverydayTemplateTesting_Static_V4-Video' THEN 'DS_01_NewPoopingEverydayTemplateTesting_Static_V4-Video'
                WHEN ad_name ~* 'DS-01_Q524TopPerformerRefreshes_V2_Capsule_NOW25-Video' THEN 'DS_01_Q524TopPerformerRefreshes_V2_Capsule_NOW25-Video'
                WHEN ad_name ~* 'DS-01_Q524TopPerformerRefreshes_V3_Capsule_Opt2_NOW25-Video' THEN 'DS_01_Q524TopPerformerRefreshes_V3_Capsule_Opt2_NOW25-Video'
                WHEN ad_name ~* 'DS-01_QualityOfLifeExploration_Static_V1_20240719_1080x1080-Video' THEN 'DS_01_QualityOfLifeExploration_Static_V1_20240719_1080x1080-Video'
                WHEN ad_name ~* 'DS-01_QualityOfLifeExploration_Static_V2_20240719_1080x1080-Video' THEN 'DS_01_QualityOfLifeExploration_Static_V2_20240719_1080x1080-Video'
                WHEN ad_name ~* 'DS-01_SurvivabilityItr_op1_20240327_1x1' THEN 'DS_01_SurvivabilityItr_op1_20240327_1x1'
                WHEN ad_name ~* 'DS-01_Top_Quote_Light_Motion_V1_20240625_1080x1920' THEN 'DS_01_Top_Quote_Light_Motion_V1_20240625_1080x1920'
                WHEN ad_name ~* 'DS01_KOLYouTubeTesting_V1_1x1_16x9-Video' THEN 'DS01_KOLYouTubeTesting_V1_1x1_16x9-Video'
                WHEN ad_name ~* 'DS01_KOLYouTubeTesting_V1_9x16_1x1_16x9-Video' THEN 'DS01_KOLYouTubeTesting_V1_9x16_1x1_16x9-Video'
                WHEN ad_name ~* 'DS01_KOLYouTubeTesting_V1_9x16-Video' THEN 'DS01_KOLYouTubeTesting_V1_9x16-Video'
                WHEN ad_name ~* 'DS01_KOLYouTubeTesting_V1_NOW25_1x1_16x9-Video' THEN 'DS01_KOLYouTubeTesting_V1_NOW25_1x1_16x9-Video'
                WHEN ad_name ~* 'DS01_KOLYouTubeTesting_V1_NOW25_AllFormats-Video' THEN 'DS01_KOLYouTubeTesting_V1_NOW25_AllFormats-Video'
                WHEN ad_name ~* 'DS01_KOLYouTubeTesting_V2_KaitDavis_AllFormats-Video' THEN 'DS01_KOLYouTubeTesting_V2_KaitDavis_AllFormats-Video'
                WHEN ad_name ~* 'DS01_KOLYouTubeTesting_V2_KaitDavis_NOW25_AllFormats-Video' THEN 'DS01_KOLYouTubeTesting_V2_KaitDavis_NOW25_AllFormats-Video'
                WHEN ad_name ~* 'DS01_KOLYouTubeTesting_V3_1080x1080-Video' THEN 'DS01_KOLYouTubeTesting_V3_1080x1080-Video'
                WHEN ad_name ~* 'DS01_KOLYouTubeTesting_V3_16x9_1x1-Video' THEN 'DS01_KOLYouTubeTesting_V3_16x9_1x1-Video'
                WHEN ad_name ~* 'DS01_KOLYouTubeTesting_V3_9x16_1x1_16x9-Video' THEN 'DS01_KOLYouTubeTesting_V3_9x16_1x1_16x9-Video'
                WHEN ad_name ~* 'DS01_KOLYouTubeTesting_V3_9x16-Video' THEN 'DS01_KOLYouTubeTesting_V3_9x16-Video'
                WHEN ad_name ~* 'GH_2in1_Lifestyle_Video_DD_40s_9x16-Video' THEN 'GH_2in1_Lifestyle_Video_DD_40s_9x16-Video'
                WHEN ad_name ~* 'GH_UGC_HopewellHeights_Video_63s_INF_9x16-Video' THEN 'GH_UGC_HopewellHeights_Video_63s_INF_9x16-Video'
                WHEN ad_name ~* 'SEED_REVEAL_30_Fem_02' THEN 'SEED_REVEAL_30_Fem_02'
                WHEN ad_name ~* 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16' THEN 'DD_SEE02_T2CListicle_PoopEveryday_V4_9x16-Video'
                WHEN ad_name ~* 'DH_2in1_Lifestyle_Video_HQ_42s_9x16' THEN 'DH_2in1_Lifestyle_Video_HQ_42s_9x16-Video'
                WHEN ad_name ~* '4.25.25_DH_Bloat_Claim_Jar_Static_HQ_V2' THEN '4.25.25_DH_Bloat_Claim_Jar_Static_HQ_V2'
                WHEN ad_name ~* 'DH_Bloat_Claim_Jar_Static_HQ' THEN 'DH_Bloat_Claim_Jar_Static_HQ'
                WHEN ad_name ~* 'DH_Gas_Claim_Capsule_Static_HQ' THEN 'DH_Gas_Claim_Capsule_Static_HQ'
                WHEN ad_name ~* 'DS-01_NewPoopingEverydayTemplateTesting_Static_V3' THEN 'DS_01_NewPoopingEverydayTemplateTesting_Static_V3-Video'
                WHEN ad_name ~* 'DH_PB_Claim_Jar_Static_HQ-Video' THEN 'DH_PB_Claim_Jar_Static_HQ-Video'
                ELSE ad_name::varchar
            END as utm_term, campaign_status, ad_status,
            COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(checkout_initiated),0) as checkout_initiated,
            COALESCE(SUM(add_to_cart),0) as add_to_cart, 0 as leads, COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM(revenue),0) as revenue, sum(0) as ft_orders, sum(0) as lt_orders
        FROM {{ source('reporting','googleads_ad_performance') }} yt
        LEFT JOIN (SELECT utm_campaign::varchar, google_campaign, COUNT(*) FROM s3_data GROUP BY 1,2) utm ON yt.campaign_name = utm.google_campaign 
        WHERE (campaign_type_custom = 'Youtube' or campaign_type_custom = 'Demand Gen')
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12),

    status_data as (
    select channel, market, product, google_campaign, utm_campaign, campaign_type, utm_content, utm_term, campaign_status, ad_status, count(*) as nb
    from platform_data 
    group by channel, market, product, google_campaign, utm_campaign, campaign_type, utm_content, utm_term, campaign_status, ad_status
    ),

    lt_data as (
    SELECT CASE WHEN channel_adj::varchar = 'Google Ads' OR channel_adj::varchar = 'Youtube' THEN 'Google Ads' ELSE channel_adj::varchar END as channel, date, date_granularity, market, product, 
            google_campaign::varchar, utm_campaign::varchar, campaign_type::varchar, 
            CASE WHEN channel_adj = 'Google Ads' OR channel_adj = 'Bing' THEN null ELSE utm_content_adj END as utm_content, 
            CASE WHEN channel_adj = 'Google Ads' OR channel_adj = 'Bing' THEN null ELSE utm_term_adj END as utm_term,
            0 as spend, 0 as impressions, 0 as clicks, 0 as checkout_initiated, 0 as add_to_cart, 0 as leads, 0 as purchases, 0 as revenue, ft_orders, lt_orders
        FROM s3_data),

    lt_final_data as (
    select * from lt_data left join status_data using(channel, market, product, google_campaign, utm_campaign, campaign_type, utm_content, utm_term)
    ),
    
    final_data as
    (SELECT channel, date::date, date_granularity, market, product, google_campaign, utm_campaign, campaign_type, utm_content, utm_term, campaign_status, ad_status,
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(checkout_initiated),0) as checkout_initiated, 
        COALESCE(SUM(add_to_cart),0) as add_to_cart, COALESCE(SUM(leads),0) as leads, COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM(revenue),0) as revenue, COALESCE(SUM(ft_orders),0) as ft_orders, COALESCE(SUM(lt_orders),0) as lt_orders
    FROM
    (SELECT channel, date::date, date_granularity, market, product, google_campaign, utm_campaign, campaign_type, utm_content, utm_term, campaign_status, ad_status,
    spend, impressions, clicks, checkout_initiated, add_to_cart, leads, purchases, revenue, ft_orders, lt_orders
    FROM platform_data 
    UNION ALL 
    SELECT channel, date::date, date_granularity, market, product, google_campaign, utm_campaign, campaign_type, utm_content, utm_term, campaign_status, ad_status,
    spend, impressions, clicks, checkout_initiated, add_to_cart, leads, purchases, revenue, ft_orders, lt_orders
    FROM lt_final_data)
    GROUP BY channel, date, date_granularity, market, product, google_campaign, utm_campaign, campaign_type, utm_content, utm_term, campaign_status, ad_status)
    
SELECT channel, 
  date, 
  date_granularity, 
  market, 
  product, 
  google_campaign,
  utm_campaign,
  campaign_status,
  campaign_type, 
  utm_content, 
  utm_term,
  ad_status,
  split_part(utm_term,'-',1) as creative,
  split_part(split_part(utm_term,'-',2),'_',1) as campaign,
  split_part(split_part(utm_term,'-',2),'_',2) as placement,
  split_part(split_part(utm_term,'-',2),'_',3) as audience,
  TO_DATE(REGEXP_SUBSTR(split_part(utm_term,'-',1), '[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{2}'), 'MM.DD.YY') AS launch_date,
  REGEXP_REPLACE(split_part(utm_term,'-',1), '^[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{2}_*', '') AS cleaned_creative,
  spend,
  impressions,
  clicks,
  add_to_cart,
  checkout_initiated,
  leads,
  purchases,
  revenue,
  ft_orders,
  lt_orders
FROM final_data
