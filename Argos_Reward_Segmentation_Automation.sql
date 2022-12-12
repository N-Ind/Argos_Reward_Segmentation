------------------------------------------------------------------------------------------------
-- Final version of the Argos Rewards Segmentation
-- Last update date: 30-11-2022
-- Author: Nils Indreiten
------------------------------------------------------------------------------------------------
-- The following is the automation that creates the Argos segmentation. The script is set to
-- run the last day of every month, and will append to the following table:
-- CUSTOMER_ANALYTICS.SANDBOX.Argos_Reward_Segmentation                                       
------------------------------------------------------------------------------------------------

USE ROLE RL_PROD_MARKETING_NCA_PII;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_X2LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA SANDBOX;

CREATE OR REPLACE PROCEDURE ARGOS_REWARD_SEGMENTATION_NI()
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
AS
$$

var SQL_COMMAND_0 = `insert overwrite into ARGOS_Seg_Dates_NI
    select FIN_PERIOD_NO, WEEK_NO, max(DATE_KEY) as UPDATE_DATE
    from ADW_PROD.INC_PL.DATE_DIM
    where RELATIVE_JS_WEEK_NUM = -1
group by 1,2;`;

var RS_0 = snowflake.execute({sqlText: SQL_COMMAND_0});

snowflake.execute({"sqlText": "set end_date = (select current_date())"});
snowflake.execute({"sqlText": "set start_date = (select min(date_key) as start_date from ADW_PROD.INC_PL.DATE_DIM where year(date_key) = 2019)"});

var SQL_COMMAND_1 = `create or replace temp table ARGOS_SEG_BASE_REDEMPTIONS as
                                           SELECT ACCOUNT_NUMBER,
                                                  TRAN_POINTS,
                                                  to_date(TRAN_DATE) as TRAN_DATE
                                           FROM NDWHS_PROD.NDWHS_PL.BURN_FACT
                                           WHERE SUPPLIER_ID = 'ARGOS'
                                             AND TRAN_POINTS > 0
                                             AND REDEMPTION_STATUS = 'RC'
                                             AND REASON_CODE = 'DL'
                                             AND TRAN_DATE between $start_date and $end_date;`;

var SQL_COMMAND_2 = `CREATE OR REPLACE TEMP TABLE ARGOS_RED_SUMMARY AS
SELECT ACCOUNT_NUMBER,
       COUNT(*)                                               AS REDS,
       SUM(TRAN_POINTS)                                       AS POINTS_RED,
       MAX(TRAN_POINTS)                                       AS LARGEST_RED,
       MAX(CASE WHEN month(TRAN_DATE) = 11 THEN 1 ELSE 0 END) AS NOV_REDER,
       MAX(CASE WHEN month(TRAN_DATE) = 12 THEN 1 ELSE 0 END) AS XMAS_REDER,
       MAX(to_date(TRAN_DATE))                                AS Most_Recent_Red
FROM ARGOS_REDS
GROUP BY ACCOUNT_NUMBER;`;

var SQL_COMMAND_3 = `create or replace temp table ARGOS_REDS_SUMMARY_GROUPS as
SELECT CASE WHEN NOV_REDER + XMAS_REDER = 0 THEN 'NO_XMAS_BF' ELSE 'XMAS_BF' END   AS XMAS_BF_REDER,
       CASE
           WHEN LARGEST_RED > 5000 THEN 'LARGE_RED'
           WHEN LARGEST_RED > 2000 THEN 'MID_RED'
           ELSE 'SMALL_RED' END                                                    AS RED_SIZE,
       CASE
           WHEN REDS > 10 THEN 'HIGH'
           WHEN REDS > 4 THEN 'MID'
           WHEN REDS > 1 THEN 'LOW'
           ELSE 'SINGLE' END                                                       AS RED_VOL,
       CASE WHEN year(Most_Recent_Red) = year($end_date) THEN 1 ELSE 0 END AS RED_THIS_YEAR,
       POINTS_RED,
       REDS,
       ACCOUNT_NUMBER
FROM ARGOS_RED_SUMMARY;`;


var SQL_COMMAND_4 = `create or replace temp table ARGOS_RED_SEGMENTS AS
SELECT     ACCOUNT_NUMBER,
           CASE WHEN RED_THIS_YEAR = 0 THEN 'INFREQUENT'
           WHEN XMAS_BF_REDER = 'XMAS_BF' AND RED_SIZE = 'LARGE_RED' THEN 'L_X_BF'
           WHEN RED_SIZE = 'LARGE_RED' THEN 'LARGE_RED'
           ELSE RED_SIZE || '_' || RED_VOL END AS ARGOS_SEGMENT
FROM ARGOS_REDS_SUMMARY_GROUPS;`;

var SQL_COMMAND_6 = `create or replace temp table PROFILE_BASE_VS_XPOPX as
SELECT  BASE_POP.ACCOUNT_NUMBER,
        ARGOS_SEGMENT AS ARGOS_SEGMENT,
        AGE_BAND,
        GENDER,
        REGION,
        ACORN_CATEGORY
FROM (SELECT ACCOUNT_NUMBER,
             ARGOS_SEGMENT
      FROM ARGOS_RED_SEGMENTS) BASE_POP
         INNER JOIN
     (SELECT ACCOUNT_NUMBER,
             AGE_BAND,
             GENDER,
             REWARD_SEGMENT,
             REGION,
             ACORN_CATEGORY,
             SSL_SEGMENT
      FROM "NDWHS_PROD"."NDWHS_PL"."CURRENT_DATA_PROFILE") CDP ON CDP.ACCOUNT_NUMBER = BASE_POP.ACCOUNT_NUMBER
GROUP BY BASE_POP.ACCOUNT_NUMBER,
       ARGOS_SEGMENT,
       AGE_BAND,
       GENDER,
       REGION,
       ACORN_CATEGORY;`;

var SQL_COMMAND_7 = `create or replace temp table SMP_PRODUCT_HIERACHY as (select a.pi_product_pk
                                                                , a.is_live
                                                                , a.short_desc
                                                                , w.pi_website_category_l4_pk
                                                                , w.pi_website_category_l4_desc
                                                                , w.pi_website_category_l3_fk
                                                                , w.pi_website_category_l3_desc
                                                                , w.pi_website_category_l2_fk
                                                                , w.pi_website_category_l2_desc
                                                                , w.pi_website_category_l1_fk
                                                                , w.pi_website_category_l1_desc
                                                                , w.pi_cmi_category_fk
                                                                , w.pi_cmi_category_desc
                                                                , w.website_category_url
                                                           from SA_ARGOS_SCV_PROD.PI_SCV.pi_product a
                                                                    left join SA_ARGOS_SCV_PROD.PI_SCV.pi_raco_subsection b
                                                                              on a.pi_raco_subsection_fk = b.pi_raco_subsection_pk
                                                                    inner join SA_ARGOS_SCV_PROD.PI_SCV.pi_website_category_l4 w
                                                                               on a.pi_website_category_l4_fk = w.pi_website_category_l4_pk
                                                           where (pi_raco_series_fk <> 135 or pi_raco_series_fk is null));`;

var SQL_COMMAND_8 = `create or replace temp table Argos_categories as
    (select distinct(pi_website_category_l3_fk) as l3_category,
                    0                           as l2_category
                                                from SMP_PRODUCT_HIERACHY
                                                order by 1);`;

var SQL_COMMAND_9 = `create or replace temp table Argos_2022_Spend as
    SELECT t.PI_ENTITY_FK,
       case when nectar_instore.pi_trans_fk is not null then 1 else 0 end as nectar_instore,
       case when nectar_online.pi_trans_pk is not null then 1 else 0 end  as nectar_online,
       case when red_instore.PI_TRANS_FK is not null then 1 else 0 end    as instore_red,
       SUM(line_total)                                                    AS spend

FROM "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_TRANS_ITEM" t
         inner join SMP_PRODUCT_HIERACHY e on t.pi_product_fk = e.pi_product_pk
         INNER JOIN adw_prod.INC_PL.DATE_DIM ON trans_date = date_key
         INNER JOIN "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_CHANNEL" c ON pi_channel_pk = pi_channel_fk
         INNER JOIN "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_TRANS_TYPE" tty ON tty.pi_trans_type_pk = t.pi_trans_type_fk
         INNER JOIN "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_PRODUCT" p ON e.pi_product_pk = p.pi_product_pk
         INNER JOIN "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_RACO_SUBSECTION" rss
                    ON pi_raco_subsection_fk = pi_raco_subsection_pk

         LEFT JOIN --join in store uses of nectar
    (SELECT DISTINCT pi_trans_fk
     FROM "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_TRANS_TENDER"
     WHERE pi_tender_type_fk = 255) AS nectar_instore ON nectar_instore.pi_trans_fk = t.pi_trans_fk

         LEFT JOIN --join in store redemption
    (SELECT DISTINCT pi_trans_fk
     FROM "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_TRANS_TENDER"
     WHERE pi_tender_type_fk = 37) AS red_instore ON red_instore.pi_trans_fk = t.pi_trans_fk

         LEFT JOIN --Join online uses of nectar
    (SELECT pi_trans_pk, TO_DATE(COALESCE(r.res_datetime, h.order_datetime)) AS order_date
     FROM sa_argos_scv_prod.pi_scv.pi_trans t
              LEFT JOIN sa_argos_scv_prod.pi_scv.pi_res r
                        ON r.pi_res_pk = t.pi_res_fk AND r.is_registered_user = 1 AND r.is_prepaid = 1
              LEFT JOIN sa_argos_scv_prod.pi_scv.pi_hd_order h
                        ON h.pi_hd_order_pk = t.hrg_order_id AND h.is_registered_user = 1
              INNER JOIN sa_argos_scv_prod.pi_scv.pi_cust_identifier w
                         ON COALESCE(r.pi_cust_identifier_fk, h.pi_cust_identifier_fk) = w.pi_cust_identifier_pk
              INNER JOIN sa_argos_scv_prod.pi_scv.pi_myaccount_nectar_card_history ON pi_account_id_fk = pi_myaccount_fk
     WHERE 1 = 1
       AND to_Date(nectar_card_link_datetime) <=
           TO_DATE(DATEADD(D, 22, COALESCE(r.res_datetime, h.order_datetime)))
       AND (to_Date(nectar_card_unlink_datetime) >= TO_DATE(COALESCE(r.res_datetime, h.order_datetime)) OR
            to_Date(nectar_card_unlink_datetime) IS NULL)
       and TRANS_DATE between $start_date and $end_date) AS nectar_online
                   ON nectar_online.pi_trans_pk = t.pi_trans_fk

WHERE pi_trans_type_desc IN
      ('Walk-in', 'FastTrack Reservation', 'Reservation Collection', 'Home Delivery', 'FastTrack Home Delivery')
  AND T.TRANS_DATE between $start_date and $end_date
  and (pi_website_category_l3_fk in (select distinct l3_category from Argos_categories where l3_category > 0)
    or pi_website_category_l2_fk = (select distinct l2_category from Argos_categories where l2_category > 0))
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 2, 3;`;


var SQL_COMMAND_10 = `create or replace temp table argos_customers as
select *
from Argos_2022_Spend
where (nectar_instore = 1 or nectar_online = 1)
  and spend > 0
  AND instore_red = 0;`;

var SQL_COMMAND_11 = `create or replace temp table argos_2022_cat as
    SELECT distinct t.PI_ENTITY_FK,
                current_date                                                                    as to_Date,
                sum(case
                        when pi_website_category_l1_desc = 'Baby and Nursery' then line_total
                        else 0 end)                                                             as baby_nursery_spend,
                sum(case
                        when pi_website_category_l1_desc = 'Appliances' then line_total
                        else 0 end)                                                             as Appliances_spend,
                sum(case
                        when pi_website_category_l1_desc = 'Clothing' then line_total
                        else 0 end)                                                             as Clothing_spend,
                sum(case
                        when pi_website_category_l1_desc in ('Home and garden', 'Garden and DIY') then line_total
                        else 0 end)                                                             as Home_garden_diy_spend,
                sum(case
                        when pi_website_category_l1_desc = 'Home and furniture' then line_total
                        else 0 end)                                                             as home_furniture_spend,
                sum(case when pi_website_category_l1_desc = 'Toys' then line_total else 0 end)  as toys_spend,
                sum(case
                        when pi_website_category_l1_desc = 'Jewellery and Watches' then line_total
                        else 0 end)                                                             as jewellery_watches_spend,
                sum(case
                        when pi_website_category_l1_desc = 'Sports and leisure' then line_total
                        else 0 end)                                                             as sports_leisure_spend,
                sum(case
                        when pi_website_category_l1_desc = 'Health and beauty' then line_total
                        else 0 end)                                                             as health_beauty_spend,
                sum(case
                        when pi_website_category_l1_desc = 'Technology' then line_total
                        else 0 end)                                                             as technology_spend,
                sum(case when pi_website_category_l1_desc = 'Gifts' then line_total else 0 end) as gifts_spend,
                SUM(line_total)                                                                 AS total_spend
FROM "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_TRANS_ITEM" t
         inner join SMP_PRODUCT_HIERACHY e on t.pi_product_fk = e.pi_product_pk
         INNER JOIN adw_prod.INC_PL.DATE_DIM ON trans_date = date_key
         INNER JOIN "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_CHANNEL" c ON pi_channel_pk = pi_channel_fk
         INNER JOIN "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_TRANS_TYPE" tty ON tty.pi_trans_type_pk = t.pi_trans_type_fk
         INNER JOIN "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_PRODUCT" p ON e.pi_product_pk = p.pi_product_pk
         INNER JOIN "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_RACO_SUBSECTION" rss
                    ON pi_raco_subsection_fk = pi_raco_subsection_pk
         inner join argos_customers cus on t.PI_ENTITY_FK = cus.pi_entity_fk
WHERE pi_trans_type_desc IN
      ('Walk-in', 'FastTrack Reservation', 'Reservation Collection', 'Home Delivery', 'FastTrack Home Delivery')
  AND T.TRANS_DATE between $start_date and $end_date
  and (pi_website_category_l3_fk in (select distinct l3_category from Argos_categories where l3_category > 0)
    or pi_website_category_l2_fk = (select distinct l2_category from Argos_categories where l2_category > 0))
GROUP BY 1, 2
HAVING  SUM(line_total)  >0
ORDER BY 1 DESC, 2, 3;`;


var SQL_COMMAND_12 = `create or replace temp table Argos_Profiled as
select distinct a.pi_entity_fk,
                case
                    when c.gender = 'F' then 1
                    when c.gender = 'M' then 2
                    else 3 end                                     as gender,
                d.current_value_score,
                case
                    when a.baby_nursery_spend = 0 then 1
                    when a.baby_nursery_spend > 0 and a.baby_nursery_spend <= 12.5 then 2
                    when a.baby_nursery_spend > 12.5 and a.baby_nursery_spend <= 23 then 3
                    when a.baby_nursery_spend > 23 and a.baby_nursery_spend <= 37 then 4
                    when a.baby_nursery_spend > 37 and a.baby_nursery_spend <= 68 then 5
                    when a.baby_nursery_spend > 68 then 6 end      as baby_nursery_spend_band,
                case
                    when a.Appliances_spend = 0 then 1
                    when a.Appliances_spend > 0 and a.Appliances_spend <= 30 then 2
                    when a.Appliances_spend > 30 and a.Appliances_spend <= 60 then 3
                    when a.Appliances_spend > 60 and a.Appliances_spend <= 120 then 4
                    when a.Appliances_spend > 120 and a.Appliances_spend <= 240 then 5
                    when a.Appliances_spend > 240 then 6 end       as Appliances_spend_band,
                case
                    when a.Clothing_spend = 0 then 1
                    when a.Clothing_spend > 0 and a.Clothing_spend <= 10 then 2
                    when a.Clothing_spend > 10 and a.Clothing_spend <= 100 then 3
                    when a.Clothing_spend > 100 then 4 end         as Clothing_spend_band,
                case
                    when a.Home_garden_diy_spend = 0 then 1
                    when a.Home_garden_diy_spend > 0 and a.Home_garden_diy_spend <= 16 then 2
                    when a.Home_garden_diy_spend > 16 and a.Home_garden_diy_spend <= 30 then 3
                    when a.Home_garden_diy_spend > 30 and a.Home_garden_diy_spend <= 54 then 4
                    when a.Home_garden_diy_spend > 54 and a.Home_garden_diy_spend <= 119 then 5
                    when a.Home_garden_diy_spend > 119 then 6 end  as Home_garden_diy_spend_band,
                case
                    when a.home_furniture_spend = 0 then 1
                    when a.home_furniture_spend > 0 and a.home_furniture_spend <= 20 then 2
                    when a.home_furniture_spend > 20 and a.home_furniture_spend <= 40 then 3
                    when a.home_furniture_spend > 40 and a.home_furniture_spend <= 79 then 4
                    when a.home_furniture_spend > 79 and a.home_furniture_spend <= 184 then 5
                    when a.home_furniture_spend > 184 then 6 end   as home_furniture_spend_band,
                case
                    when a.toys_spend = 0 then 1
                    when a.toys_spend > 0 and a.toys_spend <= 15 then 2
                    when a.toys_spend > 15 and a.toys_spend <= 30 then 3
                    when a.toys_spend > 30 and a.toys_spend <= 52 then 4
                    when a.toys_spend > 52 and a.toys_spend <= 100 then 5
                    when a.toys_spend > 100 then 6 end             as toys_spend_band,
                case
                    when a.jewellery_watches_spend = 0 then 1
                    when a.jewellery_watches_spend > 0 and a.jewellery_watches_spend <= 10 then 2
                    when a.jewellery_watches_spend > 10 and a.jewellery_watches_spend <= 20 then 3
                    when a.jewellery_watches_spend > 20 and a.jewellery_watches_spend <= 30 then 4
                    when a.jewellery_watches_spend > 30 and a.jewellery_watches_spend <= 60 then 5
                    when a.jewellery_watches_spend > 60 then 6 end as jewellery_watches_spend_band,
                case
                    when a.sports_leisure_spend = 0 then 1
                    when a.sports_leisure_spend > 0 and a.sports_leisure_spend <= 15 then 2
                    when a.sports_leisure_spend > 15 and a.sports_leisure_spend <= 25 then 3
                    when a.sports_leisure_spend > 25 and a.sports_leisure_spend <= 50 then 4
                    when a.sports_leisure_spend > 50 and a.sports_leisure_spend <= 135 then 5
                    when a.sports_leisure_spend > 135 then 6 end   as sports_leisure_spend_band,
                case
                    when a.health_beauty_spend = 0 then 1
                    when a.health_beauty_spend > 0 and a.health_beauty_spend <= 18 then 2
                    when a.health_beauty_spend > 18 and a.health_beauty_spend <= 25 then 3
                    when a.health_beauty_spend > 25 and a.health_beauty_spend <= 39 then 4
                    when a.health_beauty_spend > 39 and a.health_beauty_spend <= 64 then 5
                    when a.health_beauty_spend > 64 then 6 end     as health_beauty_spend_band,
                case
                    when a.technology_spend = 0 then 1
                    when a.technology_spend > 0 and a.technology_spend <= 30 then 2
                    when a.technology_spend > 30 and a.technology_spend <= 68 then 3
                    when a.technology_spend > 68 and a.technology_spend <= 174 then 4
                    when a.technology_spend > 174 and a.technology_spend <= 135 then 5
                    when a.technology_spend > 135 then 6 end       as technology_spend_band,
                case
                    when a.gifts_spend = 0 then 1
                    when a.gifts_spend > 0 and a.gifts_spend <= 8 then 2
                    when a.gifts_spend > 8 and a.gifts_spend <= 20 then 3
                    when a.gifts_spend > 20 and a.gifts_spend <= 32 then 4
                    when a.gifts_spend > 32 then 5 end             as gifts_spend_band
from argos_2022_cat a
         left join Argos_2022_Spend b
                   on a.pi_entity_fk = b.pi_entity_fk
         left join CUSTOMER_ANALYTICS.PRODUCTION.CVU_ARGOS_CUSTOMER_PROFILING c
                   on a.pi_entity_fk = c.entitykey
         left join (select distinct entitykey, current_value_score
                    from ADW_PROD.ADW_FEATURES_MODELS_OUTPUTS.MODEL_ARGOS_CURRENT_VALUE_SCORE
                    where year(run_date) = year($end_date)
                    and month(run_date) = month($end_date)) d
                   on a.PI_ENTITY_FK = d.entity_fk;`;

var SQL_COMMAND_13 = `create or replace temp table Argos_Segment_p1 as
                                                  select distinct a.PI_ENTITY_FK,
                                                                  map.HASH_NECTAR_COLLECTOR_CARD_NUM,
                                                                  a.CURRENT_VALUE_SCORE,
                                                                  a.BABY_NURSERY_SPEND_BAND,
                                                                  a.APPLIANCES_SPEND_BAND,
                                                                  a.CLOTHING_SPEND_BAND,
                                                                  a.HOME_GARDEN_DIY_SPEND_BAND,
                                                                  a.HOME_FURNITURE_SPEND_BAND,
                                                                  a.TOYS_SPEND_BAND,
                                                                  a.JEWELLERY_WATCHES_SPEND_BAND,
                                                                  a.SPORTS_LEISURE_SPEND_BAND,
                                                                  a.HEALTH_BEAUTY_SPEND_BAND,
                                                                  a.TECHNOLOGY_SPEND_BAND,
                                                                  a.GIFTS_SPEND_BAND,
                                                                  b.total_spend
                                                  from Argos_Profiled a
                                                           left join argos_2022_cat b
                                                                     on a.pi_entity_fk = b.PI_ENTITY_FK
                                                           inner join SA_ARGOS_SCV_PROD.PI_SCV.PI_CUST_IDENTIFIER cust --email to account
                                                                      on a.pi_entity_fk = cust.PI_entity_FK
                                                           inner join "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_MYACCOUNT_NECTAR_CARD_HISTORY" hist --account to nectar card
                                                                      on cust.PI_ACCOUNT_ID_FK = hist.PI_MYACCOUNT_FK
                                                           inner join "SA_ARGOS_SCV_PROD"."PI_SCV"."PI_NECTAR_CARD" card --nectar card to loyalty hash
                                                                      on hist.PI_NECTAR_CARD_FK = card.PI_NECTAR_CARD_PK
                                                           inner join ADW_PROD.ADW_PII_CUSTOMER_PL.DIM_NECTAR_CARD_MAPPING_PII map --loyalty hash to loyalty
                                                                      on card.NECTAR_CARD_HASHED = map.argos_hash_nectar_collector_card_num
                                                  where NECTAR_CARD_UNLINK_DATETIME is null;`;

var SQL_COMMMAND_14 = `create or replace temp table Argos_Profile_Account_Numb as (select distinct a.PI_ENTITY_FK,
                                                                            b.account_number,
                                                                            a.CURRENT_VALUE_SCORE,
                                                                            a.BABY_NURSERY_SPEND_BAND,
                                                                            a.APPLIANCES_SPEND_BAND,
                                                                            a.CLOTHING_SPEND_BAND,
                                                                            a.HOME_GARDEN_DIY_SPEND_BAND,
                                                                            a.HOME_FURNITURE_SPEND_BAND,
                                                                            a.TOYS_SPEND_BAND,
                                                                            a.JEWELLERY_WATCHES_SPEND_BAND,
                                                                            a.SPORTS_LEISURE_SPEND_BAND,
                                                                            a.HEALTH_BEAUTY_SPEND_BAND,
                                                                            a.TECHNOLOGY_SPEND_BAND,
                                                                            a.GIFTS_SPEND_BAND,
                                                                            a.total_spend
                                                            from Argos_Segment_p1 a
                                                                     inner join NDWHS_PROD.NDWHS_PL.FULL_CARD b
                                                                                on a.HASH_NECTAR_COLLECTOR_CARD_NUM = b.loyalty_id);`;

var SQL_COMMAND_15 = `create or replace table Argos_Reward_Segmentation as
                                          (select distinct      b.PI_ENTITY_FK,
                                                                a.*,
                                                                b.CURRENT_VALUE_SCORE,
                                                                b.BABY_NURSERY_SPEND_BAND,
                                                                b.APPLIANCES_SPEND_BAND,
                                                                b.CLOTHING_SPEND_BAND,
                                                                b.HOME_GARDEN_DIY_SPEND_BAND,
                                                                b.HOME_FURNITURE_SPEND_BAND,
                                                                b.TOYS_SPEND_BAND,
                                                                b.JEWELLERY_WATCHES_SPEND_BAND,
                                                                b.SPORTS_LEISURE_SPEND_BAND,
                                                                b.HEALTH_BEAUTY_SPEND_BAND,
                                                                b.TECHNOLOGY_SPEND_BAND,
                                                                b.GIFTS_SPEND_BAND,
                                                                b.total_spend,
                                                                c.total_points as points_balance,
                                                                $end_date as UPDATE_DATE
                                                from PROFILE_BASE_VS_XPOPX a
                                                         inner join Argos_Profile_Account_Numb b
                                                                    on a.ACCOUNT_NUMBER = b.account_number
                                                         left join "NDWHS_PROD"."NDWHS_PL"."CURRENT_DATA_PROFILE" c
                                                                    on a.ACCOUNT_NUMBER = c.ACCOUNT_NUMBER
                                                where b.total_spend >0);`;


var RS1 = snowflake.execute({sqlText:SQL_COMMAND_1});
var RS2 = snowflake.execute({sqlText:SQL_COMMAND_2});
var RS3 = snowflake.execute({sqlText:SQL_COMMAND_3});
var RS4 = snowflake.execute({sqlText:SQL_COMMAND_4});
var RS5 = snowflake.execute({sqlText:SQL_COMMAND_5});
var RS6 = snowflake.execute({sqlText:SQL_COMMAND_6});
var RS7 = snowflake.execute({sqlText:SQL_COMMAND_7});
var RS8 = snowflake.execute({sqlText:SQL_COMMAND_8});
var RS9 = snowflake.execute({sqlText:SQL_COMMAND_9});
var RS10 = snowflake.execute({sqlText:SQL_COMMAND_10});
var RS11 = snowflake.execute({sqlText:SQL_COMMAND_11});
var RS12 = snowflake.execute({sqlText:SQL_COMMAND_12});
var RS13 = snowflake.execute({sqlText:SQL_COMMAND_13});
var RS14 = snowflake.execute({sqlText:SQL_COMMAND_14});
var RS15 = snowflake.execute({sqlText:SQL_COMMAND_15});


return "END";

$$;


CREATE OR REPLACE TASK ARGOS_REWARD_SEGMENTATION_NI
    WAREHOUSE = WHS_PROD_MARKETING_ANALYTICS_X2LARGE
    SCHEDULE = 'USING CRON 00 09 L * * GMT'
    AS
    CALL ARGOS_REWARD_SEGMENTATION_NI();


-- set task active
ALTER TASK ARGOS_REWARD_SEGMENTATION_NI RESUME;

select *
from table (information_schema.task_history(
        scheduled_time_range_start => dateadd('hours', -1, current_timestamp()),
        task_name => 'ARGOS_REWARD_SEGMENTATION_NI'));



