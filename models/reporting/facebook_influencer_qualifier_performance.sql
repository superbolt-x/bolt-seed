{{ config (
    alias = target.database + '_facebook_influencer_qualifier_performance'
)}}

select *, CASE 
        WHEN qualifier = 'Married' THEN 1
        WHEN qualifier = 'Not Married (note: all - no kids)' THEN 2
        WHEN qualifier = 'Married, excl Bobby and Shred' THEN 3
        WHEN qualifier = 'Married + no kids' THEN 4
        WHEN qualifier = 'Married + no kids, excl Shred' THEN 5
        WHEN qualifier = 'Kids' THEN 6
        WHEN qualifier = 'No Kids' THEN 7
        WHEN qualifier = 'Kids, excl Bobby' THEN 8
        WHEN qualifier = 'No Kids, excl Shred' THEN 9
        WHEN qualifier = 'East' THEN 10
        WHEN qualifier = 'South' THEN 11
        WHEN qualifier = 'West' THEN 12
        WHEN qualifier = '25-34' THEN 13
        WHEN qualifier = '35-44' THEN 14
        WHEN qualifier = '45+' THEN 15
        WHEN qualifier = 'Lifestyle' THEN 16
        WHEN qualifier = 'Lifestyle + Kids' THEN 17
        WHEN qualifier = 'Lifestyle + No Kids' THEN 18
        WHEN qualifier = 'Lifestyle + Married' THEN 19
        WHEN qualifier = 'Lifestyle + Not Married' THEN 20
        WHEN qualifier = 'Opposites of Bobby and Shred, Aggreagate' THEN 21
        ELSE NULL
    END AS category_order from 
    (select 
case when ad_name ~* 'KaitDavis|MichelJanse|JuliaHavens|LizAdams' then 'South'
     when ad_name ~* 'MeganRoupe|VivianeAudi|MecahWihrt|NicoleCogan|BeccaTilley|HayleyKiyoko|JillianGottlieb|KaylaSeah' then 'West' 
     end as qualifier,
CASE WHEN ad_name ~* 'Bobby' THEN 'Bobby' 
        WHEN ad_name ~* 'AnnaMaeGroves' THEN 'AnnaMaeGroves' 
        WHEN ad_name ~* 'BeccaTilley' THEN 'BeccaTilley' 
        WHEN ad_name ~* 'HayleyKiyoko' THEN 'HayleyKiyoko' 
        WHEN ad_name ~* 'JillianGottlieb' THEN 'JillianGottlieb' 
        WHEN ad_name ~* 'JuliaHavens' THEN 'JuliaHavens' 
        WHEN ad_name ~* 'KaitDavis' THEN 'KaitDavis' 
        WHEN ad_name ~* 'KaylaSeah' THEN 'KaylaSeah' 
        WHEN ad_name ~* 'LizAdams' THEN 'LizAdams' 
        WHEN ad_name ~* 'MecahWihrt' THEN 'MecahWihrt' 
        WHEN ad_name ~* 'MeganRoupe' THEN 'MeganRoupe' 
        WHEN ad_name ~* 'MichelJanse' THEN 'MichelJanse' 
        WHEN ad_name ~* 'NicoleCogan' THEN 'NicoleCogan' 
        WHEN ad_name ~* 'Shred' THEN 'Shred' 
        WHEN ad_name ~* 'VivianeAudi' THEN 'VivianeAudi' end as name,age,gender,
SUM(coalesce(spend,0)) as spend,
SUM(coalesce(inline_link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
SUM(coalesce(purchases,0)) as purchases
FROM {{ source('facebook_raw','ads_insights_age_gender') }}
LEFT JOIN 
    (SELECT date, ad_id, _fivetran_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
    FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2,3) USING (date,ad_id, _fivetran_id)
where date >='Jan 1,2024' and date <= 'Mar 24,2024' and qualifier is not null and campaign_name = 'DS01 - Prospect - A+SC Campaign - KOL Campaign'
group by 1,2,3,4

union all

select 
case when ad_name ~* 'Shred|Bobby|JillianGottlieb' then 'East' end as qualifier,
CASE 
        WHEN ad_name ~* 'Bobby' THEN 'Bobby' 
        WHEN ad_name ~* 'AnnaMaeGroves' THEN 'AnnaMaeGroves' 
        WHEN ad_name ~* 'BeccaTilley' THEN 'BeccaTilley' 
        WHEN ad_name ~* 'HayleyKiyoko' THEN 'HayleyKiyoko' 
        WHEN ad_name ~* 'JillianGottlieb' THEN 'JillianGottlieb' 
        WHEN ad_name ~* 'JuliaHavens' THEN 'JuliaHavens' 
        WHEN ad_name ~* 'KaitDavis' THEN 'KaitDavis' 
        WHEN ad_name ~* 'KaylaSeah' THEN 'KaylaSeah' 
        WHEN ad_name ~* 'LizAdams' THEN 'LizAdams' 
        WHEN ad_name ~* 'MecahWihrt' THEN 'MecahWihrt' 
        WHEN ad_name ~* 'MeganRoupe' THEN 'MeganRoupe' 
        WHEN ad_name ~* 'MichelJanse' THEN 'MichelJanse' 
        WHEN ad_name ~* 'NicoleCogan' THEN 'NicoleCogan' 
        WHEN ad_name ~* 'Shred' THEN 'Shred' 
        WHEN ad_name ~* 'VivianeAudi' THEN 'VivianeAudi' end as name,age,gender,
SUM(coalesce(spend,0)) as spend,
SUM(coalesce(inline_link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
SUM(coalesce(purchases,0)) as purchases
FROM {{ source('facebook_raw','ads_insights_age_gender') }}
LEFT JOIN 
    (SELECT date, ad_id, _fivetran_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
    FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2,3) USING (date,ad_id, _fivetran_id)
where date >='Jan 1,2024' and date <= 'Mar 24,2024' and qualifier is not null and campaign_name = 'DS01 - Prospect - A+SC Campaign - KOL Campaign'
group by 1,2,3,4

union all

select 
case when ad_name ~* 'Shred|Bobby' then '45+'
     when ad_name ~* 'AnnaMaeGroves|JillianGottlieb' then '35-44'
     when ad_name ~* 'KaitDavis|MichelJanse|MeganRoupe|VivianeAudi|JuliaHavens|MecahWihrt|LizAdams|NicoleCogan|BeccaTilley|HayleyKiyoko|KaylaSeah' then '25-34' end as qualifier,
CASE 
        WHEN ad_name ~* 'Bobby' THEN 'Bobby' 
        WHEN ad_name ~* 'AnnaMaeGroves' THEN 'AnnaMaeGroves' 
        WHEN ad_name ~* 'BeccaTilley' THEN 'BeccaTilley' 
        WHEN ad_name ~* 'HayleyKiyoko' THEN 'HayleyKiyoko' 
        WHEN ad_name ~* 'JillianGottlieb' THEN 'JillianGottlieb' 
        WHEN ad_name ~* 'JuliaHavens' THEN 'JuliaHavens' 
        WHEN ad_name ~* 'KaitDavis' THEN 'KaitDavis' 
        WHEN ad_name ~* 'KaylaSeah' THEN 'KaylaSeah' 
        WHEN ad_name ~* 'LizAdams' THEN 'LizAdams' 
        WHEN ad_name ~* 'MecahWihrt' THEN 'MecahWihrt' 
        WHEN ad_name ~* 'MeganRoupe' THEN 'MeganRoupe' 
        WHEN ad_name ~* 'MichelJanse' THEN 'MichelJanse' 
        WHEN ad_name ~* 'NicoleCogan' THEN 'NicoleCogan' 
        WHEN ad_name ~* 'Shred' THEN 'Shred' 
        WHEN ad_name ~* 'VivianeAudi' THEN 'VivianeAudi' end as name,age,gender,
SUM(coalesce(spend,0)) as spend,
SUM(coalesce(inline_link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
SUM(coalesce(purchases,0)) as purchases
FROM {{ source('facebook_raw','ads_insights_age_gender') }}
LEFT JOIN 
    (SELECT date, ad_id, _fivetran_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
    FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2,3) USING (date,ad_id, _fivetran_id)
where date >='Jan 1,2024' and date <= 'Mar 24,2024' and qualifier is not null and campaign_name = 'DS01 - Prospect - A+SC Campaign - KOL Campaign'
group by 1,2,3,4

union all

select 
case when ad_name ~* 'Bobby|MeganRoupe|JuliaHavens|AnnaMaeGroves|LizAdams' then 'Kids'
     when ad_name ~* 'KaitDavis|Shred|MichelJanse|VivianeAudi|MecahWihrt|NicoleCogan|BeccaTilley|HayleyKiyoko|JillianGottlieb|KaylaSeah' then 'No Kids' end as qualifier,
CASE 
        WHEN ad_name ~* 'Bobby' THEN 'Bobby' 
        WHEN ad_name ~* 'AnnaMaeGroves' THEN 'AnnaMaeGroves' 
        WHEN ad_name ~* 'BeccaTilley' THEN 'BeccaTilley' 
        WHEN ad_name ~* 'HayleyKiyoko' THEN 'HayleyKiyoko' 
        WHEN ad_name ~* 'JillianGottlieb' THEN 'JillianGottlieb' 
        WHEN ad_name ~* 'JuliaHavens' THEN 'JuliaHavens' 
        WHEN ad_name ~* 'KaitDavis' THEN 'KaitDavis' 
        WHEN ad_name ~* 'KaylaSeah' THEN 'KaylaSeah' 
        WHEN ad_name ~* 'LizAdams' THEN 'LizAdams' 
        WHEN ad_name ~* 'MecahWihrt' THEN 'MecahWihrt' 
        WHEN ad_name ~* 'MeganRoupe' THEN 'MeganRoupe' 
        WHEN ad_name ~* 'MichelJanse' THEN 'MichelJanse' 
        WHEN ad_name ~* 'NicoleCogan' THEN 'NicoleCogan' 
        WHEN ad_name ~* 'Shred' THEN 'Shred' 
        WHEN ad_name ~* 'VivianeAudi' THEN 'VivianeAudi' end as name,age,gender,
SUM(coalesce(spend,0)) as spend,
SUM(coalesce(inline_link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
SUM(coalesce(purchases,0)) as purchases
FROM {{ source('facebook_raw','ads_insights_age_gender') }}
LEFT JOIN 
    (SELECT date, ad_id, _fivetran_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
    FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2,3) USING (date,ad_id, _fivetran_id)
where date >='Jan 1,2024' and date <= 'Mar 24,2024' and qualifier is not null and campaign_name = 'DS01 - Prospect - A+SC Campaign - KOL Campaign'
group by 1,2,3,4

union all

select 
case when ad_name ~* 'Bobby|MeganRoupe|JuliaHavens|AnnaMaeGroves|LizAdams' then 'Kids, excl Bobby'
     when ad_name ~* 'KaitDavis|Shred|MichelJanse|VivianeAudi|MecahWihrt|NicoleCogan|BeccaTilley|HayleyKiyoko|JillianGottlieb|KaylaSeah' then 'No Kids, excl Shred' end as qualifier,
CASE 
        WHEN ad_name ~* 'Bobby' THEN 'Bobby' 
        WHEN ad_name ~* 'AnnaMaeGroves' THEN 'AnnaMaeGroves' 
        WHEN ad_name ~* 'BeccaTilley' THEN 'BeccaTilley' 
        WHEN ad_name ~* 'HayleyKiyoko' THEN 'HayleyKiyoko' 
        WHEN ad_name ~* 'JillianGottlieb' THEN 'JillianGottlieb' 
        WHEN ad_name ~* 'JuliaHavens' THEN 'JuliaHavens' 
        WHEN ad_name ~* 'KaitDavis' THEN 'KaitDavis' 
        WHEN ad_name ~* 'KaylaSeah' THEN 'KaylaSeah' 
        WHEN ad_name ~* 'LizAdams' THEN 'LizAdams' 
        WHEN ad_name ~* 'MecahWihrt' THEN 'MecahWihrt' 
        WHEN ad_name ~* 'MeganRoupe' THEN 'MeganRoupe' 
        WHEN ad_name ~* 'MichelJanse' THEN 'MichelJanse' 
        WHEN ad_name ~* 'NicoleCogan' THEN 'NicoleCogan' 
        WHEN ad_name ~* 'Shred' THEN 'Shred' 
        WHEN ad_name ~* 'VivianeAudi' THEN 'VivianeAudi' end as name,age,gender,
SUM(coalesce(spend,0)) as spend,
SUM(coalesce(inline_link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
SUM(coalesce(purchases,0)) as purchases
FROM {{ source('facebook_raw','ads_insights_age_gender') }}
LEFT JOIN 
    (SELECT date, ad_id, _fivetran_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
    FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2,3) USING (date,ad_id, _fivetran_id)
where date >='Jan 1,2024' and date <= 'Mar 24,2024' and qualifier is not null and campaign_name = 'DS01 - Prospect - A+SC Campaign - KOL Campaign'
and ad_name !~* 'Bobby|Shred'
group by 1,2,3,4

union all

select 
case when ad_name ~* 'KaitDavis|Shred|Bobby|MichelJanse|MeganRoupe|JuliaHavens|MecahWihrt|AnnaMaeGroves|LizAdams|NicoleCogan' then 'Married'
     when ad_name ~* 'VivianeAudi|BeccaTilley|HayleyKiyoko' then 'Not Married (note: all - no kids)' end as qualifier,
CASE 
        WHEN ad_name ~* 'Bobby' THEN 'Bobby' 
        WHEN ad_name ~* 'AnnaMaeGroves' THEN 'AnnaMaeGroves' 
        WHEN ad_name ~* 'BeccaTilley' THEN 'BeccaTilley' 
        WHEN ad_name ~* 'HayleyKiyoko' THEN 'HayleyKiyoko' 
        WHEN ad_name ~* 'JillianGottlieb' THEN 'JillianGottlieb' 
        WHEN ad_name ~* 'JuliaHavens' THEN 'JuliaHavens' 
        WHEN ad_name ~* 'KaitDavis' THEN 'KaitDavis' 
        WHEN ad_name ~* 'KaylaSeah' THEN 'KaylaSeah' 
        WHEN ad_name ~* 'LizAdams' THEN 'LizAdams' 
        WHEN ad_name ~* 'MecahWihrt' THEN 'MecahWihrt' 
        WHEN ad_name ~* 'MeganRoupe' THEN 'MeganRoupe' 
        WHEN ad_name ~* 'MichelJanse' THEN 'MichelJanse' 
        WHEN ad_name ~* 'NicoleCogan' THEN 'NicoleCogan' 
        WHEN ad_name ~* 'Shred' THEN 'Shred' 
        WHEN ad_name ~* 'VivianeAudi' THEN 'VivianeAudi' end as name,age,gender,
SUM(coalesce(spend,0)) as spend,
SUM(coalesce(inline_link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
SUM(coalesce(purchases,0)) as purchases
FROM {{ source('facebook_raw','ads_insights_age_gender') }}
LEFT JOIN 
    (SELECT date, ad_id, _fivetran_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
    FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2,3) USING (date,ad_id, _fivetran_id)
where date >='Jan 1,2024' and date <= 'Mar 24,2024' and qualifier is not null and campaign_name = 'DS01 - Prospect - A+SC Campaign - KOL Campaign'
group by 1,2,3,4

union all

select 
case when ad_name ~* 'Bobby|MeganRoupe|JuliaHavens|AnnaMaeGroves|LizAdams' then 'Lifestyle + Kids'
     when ad_name ~* 'KaitDavis|Shred|MichelJanse|VivianeAudi|MecahWihrt|NicoleCogan|BeccaTilley|HayleyKiyoko|JillianGottlieb|KaylaSeah' then 'Lifestyle + No Kids' end as qualifier,
CASE 
        WHEN ad_name ~* 'Bobby' THEN 'Bobby' 
        WHEN ad_name ~* 'AnnaMaeGroves' THEN 'AnnaMaeGroves' 
        WHEN ad_name ~* 'BeccaTilley' THEN 'BeccaTilley' 
        WHEN ad_name ~* 'HayleyKiyoko' THEN 'HayleyKiyoko' 
        WHEN ad_name ~* 'JillianGottlieb' THEN 'JillianGottlieb' 
        WHEN ad_name ~* 'JuliaHavens' THEN 'JuliaHavens' 
        WHEN ad_name ~* 'KaitDavis' THEN 'KaitDavis' 
        WHEN ad_name ~* 'KaylaSeah' THEN 'KaylaSeah' 
        WHEN ad_name ~* 'LizAdams' THEN 'LizAdams' 
        WHEN ad_name ~* 'MecahWihrt' THEN 'MecahWihrt' 
        WHEN ad_name ~* 'MeganRoupe' THEN 'MeganRoupe' 
        WHEN ad_name ~* 'MichelJanse' THEN 'MichelJanse' 
        WHEN ad_name ~* 'NicoleCogan' THEN 'NicoleCogan' 
        WHEN ad_name ~* 'Shred' THEN 'Shred' 
        WHEN ad_name ~* 'VivianeAudi' THEN 'VivianeAudi' end as name,age,gender,
SUM(coalesce(spend,0)) as spend,
SUM(coalesce(inline_link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
SUM(coalesce(purchases,0)) as purchases
FROM {{ source('facebook_raw','ads_insights_age_gender') }}
LEFT JOIN 
    (SELECT date, ad_id, _fivetran_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
    FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2,3) USING (date,ad_id, _fivetran_id)
where date >='Jan 1,2024' and date <= 'Mar 24,2024' and qualifier is not null and campaign_name = 'DS01 - Prospect - A+SC Campaign - KOL Campaign'
and ad_name ~* 'KaitDavis|MichelJanse|VivianeAudi|JuliaHavens|MecahWihrt|AnnaMaeGroves|LizAdams|NicoleCogan|BeccaTilley|HayleyKiyoko|KaylaSeah'
group by 1,2,3,4

union all

select 
case when ad_name ~* 'KaitDavis|MichelJanse|VivianeAudi|JuliaHavens|MecahWihrt|AnnaMaeGroves|LizAdams|NicoleCogan|BeccaTilley|HayleyKiyoko|KaylaSeah' then 'Lifestyle' end as qualifier,
CASE 
        WHEN ad_name ~* 'Bobby' THEN 'Bobby' 
        WHEN ad_name ~* 'AnnaMaeGroves' THEN 'AnnaMaeGroves' 
        WHEN ad_name ~* 'BeccaTilley' THEN 'BeccaTilley' 
        WHEN ad_name ~* 'HayleyKiyoko' THEN 'HayleyKiyoko' 
        WHEN ad_name ~* 'JillianGottlieb' THEN 'JillianGottlieb' 
        WHEN ad_name ~* 'JuliaHavens' THEN 'JuliaHavens' 
        WHEN ad_name ~* 'KaitDavis' THEN 'KaitDavis' 
        WHEN ad_name ~* 'KaylaSeah' THEN 'KaylaSeah' 
        WHEN ad_name ~* 'LizAdams' THEN 'LizAdams' 
        WHEN ad_name ~* 'MecahWihrt' THEN 'MecahWihrt' 
        WHEN ad_name ~* 'MeganRoupe' THEN 'MeganRoupe' 
        WHEN ad_name ~* 'MichelJanse' THEN 'MichelJanse' 
        WHEN ad_name ~* 'NicoleCogan' THEN 'NicoleCogan' 
        WHEN ad_name ~* 'Shred' THEN 'Shred' 
        WHEN ad_name ~* 'VivianeAudi' THEN 'VivianeAudi' end as name,age,gender,
SUM(coalesce(spend,0)) as spend,
SUM(coalesce(inline_link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
SUM(coalesce(purchases,0)) as purchases
FROM {{ source('facebook_raw','ads_insights_age_gender') }}
LEFT JOIN 
    (SELECT date, ad_id, _fivetran_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
    FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2,3) USING (date,ad_id, _fivetran_id)
where date >='Jan 1,2024' and date <= 'Mar 24,2024' and qualifier is not null and campaign_name = 'DS01 - Prospect - A+SC Campaign - KOL Campaign'
group by 1,2,3,4

union all

select 
case when ad_name ~* 'KaitDavis|MichelJanse|JuliaHavens|MecahWihrt|AnnaMaeGroves|LizAdams|NicoleCogan' then 'Lifestyle + Married'
     when ad_name ~* 'VivianeAudi|BeccaTilley|HayleyKiyoko|KaylaSeah' then 'Lifestyle + Not Married' end as qualifier,
CASE 
        WHEN ad_name ~* 'Bobby' THEN 'Bobby' 
        WHEN ad_name ~* 'AnnaMaeGroves' THEN 'AnnaMaeGroves' 
        WHEN ad_name ~* 'BeccaTilley' THEN 'BeccaTilley' 
        WHEN ad_name ~* 'HayleyKiyoko' THEN 'HayleyKiyoko' 
        WHEN ad_name ~* 'JillianGottlieb' THEN 'JillianGottlieb' 
        WHEN ad_name ~* 'JuliaHavens' THEN 'JuliaHavens' 
        WHEN ad_name ~* 'KaitDavis' THEN 'KaitDavis' 
        WHEN ad_name ~* 'KaylaSeah' THEN 'KaylaSeah' 
        WHEN ad_name ~* 'LizAdams' THEN 'LizAdams' 
        WHEN ad_name ~* 'MecahWihrt' THEN 'MecahWihrt' 
        WHEN ad_name ~* 'MeganRoupe' THEN 'MeganRoupe' 
        WHEN ad_name ~* 'MichelJanse' THEN 'MichelJanse' 
        WHEN ad_name ~* 'NicoleCogan' THEN 'NicoleCogan' 
        WHEN ad_name ~* 'Shred' THEN 'Shred' 
        WHEN ad_name ~* 'VivianeAudi' THEN 'VivianeAudi' end as name,age,gender,
SUM(coalesce(spend,0)) as spend,
SUM(coalesce(inline_link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
SUM(coalesce(purchases,0)) as purchases
FROM {{ source('facebook_raw','ads_insights_age_gender') }}
LEFT JOIN 
    (SELECT date, ad_id, _fivetran_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
    FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2,3) USING (date,ad_id, _fivetran_id)
where date >='Jan 1,2024' and date <= 'Mar 24,2024' and qualifier is not null and campaign_name = 'DS01 - Prospect - A+SC Campaign - KOL Campaign'
group by 1,2,3,4

union all

select 
case when ad_name ~* 'VivianeAudi|BeccaTilley|HayleyKiyoko|JillianGottlieb|KaylaSeah' then 'Opposites of Bobby and Shred, Aggreagate' end as qualifier,
CASE 
        WHEN ad_name ~* 'Bobby' THEN 'Bobby' 
        WHEN ad_name ~* 'AnnaMaeGroves' THEN 'AnnaMaeGroves' 
        WHEN ad_name ~* 'BeccaTilley' THEN 'BeccaTilley' 
        WHEN ad_name ~* 'HayleyKiyoko' THEN 'HayleyKiyoko' 
        WHEN ad_name ~* 'JillianGottlieb' THEN 'JillianGottlieb' 
        WHEN ad_name ~* 'JuliaHavens' THEN 'JuliaHavens' 
        WHEN ad_name ~* 'KaitDavis' THEN 'KaitDavis' 
        WHEN ad_name ~* 'KaylaSeah' THEN 'KaylaSeah' 
        WHEN ad_name ~* 'LizAdams' THEN 'LizAdams' 
        WHEN ad_name ~* 'MecahWihrt' THEN 'MecahWihrt' 
        WHEN ad_name ~* 'MeganRoupe' THEN 'MeganRoupe' 
        WHEN ad_name ~* 'MichelJanse' THEN 'MichelJanse' 
        WHEN ad_name ~* 'NicoleCogan' THEN 'NicoleCogan' 
        WHEN ad_name ~* 'Shred' THEN 'Shred' 
        WHEN ad_name ~* 'VivianeAudi' THEN 'VivianeAudi' end as name,age,gender,
SUM(coalesce(spend,0)) as spend,
SUM(coalesce(inline_link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
SUM(coalesce(purchases,0)) as purchases
FROM {{ source('facebook_raw','ads_insights_age_gender') }}
LEFT JOIN 
    (SELECT date, ad_id, _fivetran_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
    FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2,3) USING (date,ad_id, _fivetran_id)
where date >='Jan 1,2024' and date <= 'Mar 24,2024' and qualifier is not null and campaign_name = 'DS01 - Prospect - A+SC Campaign - KOL Campaign'
group by 1,2,3,4

union all

select 
case when ad_name ~* 'KaitDavis|MichelJanse|MeganRoupe|JuliaHavens|MecahWihrt|AnnaMaeGroves|LizAdams|NicoleCogan' then 'Married, excl Bobby and Shred' end as qualifier,
CASE 
        WHEN ad_name ~* 'Bobby' THEN 'Bobby' 
        WHEN ad_name ~* 'AnnaMaeGroves' THEN 'AnnaMaeGroves' 
        WHEN ad_name ~* 'BeccaTilley' THEN 'BeccaTilley' 
        WHEN ad_name ~* 'HayleyKiyoko' THEN 'HayleyKiyoko' 
        WHEN ad_name ~* 'JillianGottlieb' THEN 'JillianGottlieb' 
        WHEN ad_name ~* 'JuliaHavens' THEN 'JuliaHavens' 
        WHEN ad_name ~* 'KaitDavis' THEN 'KaitDavis' 
        WHEN ad_name ~* 'KaylaSeah' THEN 'KaylaSeah' 
        WHEN ad_name ~* 'LizAdams' THEN 'LizAdams' 
        WHEN ad_name ~* 'MecahWihrt' THEN 'MecahWihrt' 
        WHEN ad_name ~* 'MeganRoupe' THEN 'MeganRoupe' 
        WHEN ad_name ~* 'MichelJanse' THEN 'MichelJanse' 
        WHEN ad_name ~* 'NicoleCogan' THEN 'NicoleCogan' 
        WHEN ad_name ~* 'Shred' THEN 'Shred' 
        WHEN ad_name ~* 'VivianeAudi' THEN 'VivianeAudi' end as name,age,gender,
SUM(coalesce(spend,0)) as spend,
SUM(coalesce(inline_link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
SUM(coalesce(purchases,0)) as purchases
FROM {{ source('facebook_raw','ads_insights_age_gender') }}
LEFT JOIN 
    (SELECT date, ad_id, _fivetran_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
    FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2,3) USING (date,ad_id, _fivetran_id)
where date >='Jan 1,2024' and date <= 'Mar 24,2024' and qualifier is not null and campaign_name = 'DS01 - Prospect - A+SC Campaign - KOL Campaign'
group by 1,2,3,4

union all

select 
case when ad_name ~* 'KaitDavis|Shred|MichelJanse|MecahWihrt|NicoleCogan' then 'Married + no kids' end as qualifier,
CASE 
        WHEN ad_name ~* 'Bobby' THEN 'Bobby' 
        WHEN ad_name ~* 'AnnaMaeGroves' THEN 'AnnaMaeGroves' 
        WHEN ad_name ~* 'BeccaTilley' THEN 'BeccaTilley' 
        WHEN ad_name ~* 'HayleyKiyoko' THEN 'HayleyKiyoko' 
        WHEN ad_name ~* 'JillianGottlieb' THEN 'JillianGottlieb' 
        WHEN ad_name ~* 'JuliaHavens' THEN 'JuliaHavens' 
        WHEN ad_name ~* 'KaitDavis' THEN 'KaitDavis' 
        WHEN ad_name ~* 'KaylaSeah' THEN 'KaylaSeah' 
        WHEN ad_name ~* 'LizAdams' THEN 'LizAdams' 
        WHEN ad_name ~* 'MecahWihrt' THEN 'MecahWihrt' 
        WHEN ad_name ~* 'MeganRoupe' THEN 'MeganRoupe' 
        WHEN ad_name ~* 'MichelJanse' THEN 'MichelJanse' 
        WHEN ad_name ~* 'NicoleCogan' THEN 'NicoleCogan' 
        WHEN ad_name ~* 'Shred' THEN 'Shred' 
        WHEN ad_name ~* 'VivianeAudi' THEN 'VivianeAudi' end as name,age,gender,
SUM(coalesce(spend,0)) as spend,
SUM(coalesce(inline_link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
SUM(coalesce(purchases,0)) as purchases
FROM {{ source('facebook_raw','ads_insights_age_gender') }}
LEFT JOIN 
    (SELECT date, ad_id, _fivetran_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
    FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2,3) USING (date,ad_id, _fivetran_id)
where date >='Jan 1,2024' and date <= 'Mar 24,2024' and qualifier is not null and campaign_name = 'DS01 - Prospect - A+SC Campaign - KOL Campaign'
group by 1,2,3,4

union all

select 
case when ad_name ~* 'KaitDavis|MichelJanse|MecahWihrt|NicoleCogan' then 'Married + no kids, excl Shred' end as qualifier,
CASE 
        WHEN ad_name ~* 'Bobby' THEN 'Bobby' 
        WHEN ad_name ~* 'AnnaMaeGroves' THEN 'AnnaMaeGroves' 
        WHEN ad_name ~* 'BeccaTilley' THEN 'BeccaTilley' 
        WHEN ad_name ~* 'HayleyKiyoko' THEN 'HayleyKiyoko' 
        WHEN ad_name ~* 'JillianGottlieb' THEN 'JillianGottlieb' 
        WHEN ad_name ~* 'JuliaHavens' THEN 'JuliaHavens' 
        WHEN ad_name ~* 'KaitDavis' THEN 'KaitDavis' 
        WHEN ad_name ~* 'KaylaSeah' THEN 'KaylaSeah' 
        WHEN ad_name ~* 'LizAdams' THEN 'LizAdams' 
        WHEN ad_name ~* 'MecahWihrt' THEN 'MecahWihrt' 
        WHEN ad_name ~* 'MeganRoupe' THEN 'MeganRoupe' 
        WHEN ad_name ~* 'MichelJanse' THEN 'MichelJanse' 
        WHEN ad_name ~* 'NicoleCogan' THEN 'NicoleCogan' 
        WHEN ad_name ~* 'Shred' THEN 'Shred' 
        WHEN ad_name ~* 'VivianeAudi' THEN 'VivianeAudi' end as name,age,gender,
SUM(coalesce(spend,0)) as spend,
SUM(coalesce(inline_link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
SUM(coalesce(purchases,0)) as purchases
FROM {{ source('facebook_raw','ads_insights_age_gender') }}
LEFT JOIN 
    (SELECT date, ad_id, _fivetran_id, COALESCE(SUM(CASE WHEN action_type = 'purchase' THEN value END),0) as purchases 
    FROM {{ source('facebook_raw','ads_insights_age_gender_actions') }} GROUP BY 1,2,3) USING (date,ad_id, _fivetran_id)
where date >='Jan 1,2024' and date <= 'Mar 24,2024' and qualifier is not null and campaign_name = 'DS01 - Prospect - A+SC Campaign - KOL Campaign'
group by 1,2,3,4)
order by category_order
