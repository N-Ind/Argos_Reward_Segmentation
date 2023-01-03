# This is the repository for the Argos reward segmentation

## Objective

The aim of this project is to come up with a rewards segmentation for Argos, based on several factors. The segmentation is broken down into two main streams, 
this is due to the nature of the data and data infrastructure. The first is concerned with data coming from NDWS_PROD.NDWHS_PL.BURN_FACT, whereby the points 
redemption data for Argos exists at the account number level. The second is from the PI_SCV family of tables which contain transaction information for Argos 
at the PI_ENTITY_FK level. 

The segmentation will inform future CRM activities and will prove useful for targeting purposes. 

## Segmentation Overview

### Part I

1.1

Involves creating the segmentation groups at an individual level, the categories are created according to the following:

•	Number of redemptions

•	Total points redeemed

•	The maximum points redeemed 

•	Whether the account is a November redeemer

•	Whether the account is a Christmas redeemer

•	The most recent redemption

1.2

The group categories are then summarised at the group level, for a classification of the following:

•	XMAS_BF_REDER: denotes whether the group redeems during Christmas

•	RED_SIZE: denotes the size of the group’s largest redemption, where the largest redemption is greater than 5000 then the category is LARGE_RED, when the redemption is greater than 2000 the category is MID_RED, and the final category is SMALL_RED.

•	RED_VOL: denotes the number of redemptions, classified into High when the redemptions are greater than 10, Mid when greater than 4, Low when greater than 1, and Single redemption when there is only one redemption. 

•	RED_THIS_YEAR: denotes whether the account has redeemed within the past year.


1.3 

The above grouping serves as the basis for the first tier of the segmentation, the segments are like the groups above, however they are broken down into the following redemption segments:

•	INFREQUENT

•	L_X_BF

•	LARGE_RED

•	A variable combining the redemption_size and red_vol. For example, if the red_size (from 1.2) is SMALL_RED and the RED_VOL is SINGLE, then the ARGOS_SEGMENT will be SMALL_RED_SINGLE. 

The segmentation is iterated upon to build a customer profile, this involves a combination of the ARGOS_SEGMENT developed in the previous parts and overlaying that information onto demographic variables, such as gender and age. 


### Part II

2.1 

To make the segmentation more holistic and robust, we sought to incorporate purchasing behaviours at the individual consumer and product category level. This is the second stream where the customer information exists at the PI_ENTITY_FK level. 
The transaction at Argos data was restricted to only include Nectar members. 

2.2 

For the purposes of the segmentation, level 3 of the Argos product hierarchies is the one that will be used. The total spend per category is broken down for each customer, resulting in the following categories:

•	Baby_nursery_spend

•	Applieances_spend

•	Clothing_spend

•	Home_garden_diy_spend

•	Home_furniture_spend

•	Toys_spend

•	Jewellery_watches_spend

•	Sports_leisure_spend

•	Health_beauty_spend

•	Technology_spend

•	Gifts_spend

•	Total_spend

Based on the total category spend customers were broken down into NTILES, to put them into spending bands. In addition, demographic level information along with CVS scores that exist at the pi_entity_fk level from the ARGOS side of the data infrastructure. The final Argos side profiled table contains the following information:

•	Age band

•	Gender

•	Current value score

•	Baby nursery spend band

•	Appliances spend band

•	Clothing spend band

•	Home garden diy spend band

•	Home furniture spend band

•	Toys spend band

•	Jewellery watches spend band

•	Sports leisure spend band

•	Technology spend band

•	Gifts spend band


### Part III

3.1 

Now that the final segmented/profiled customers exist in both streams, it’s time to bring them together. Here are where some issues matching accounts across streams are encountered. The reason is because the first stream exists at the account number level, 
whereas the pi_entity_fk level data can only be linked back at the loyalty_id level. Although in theory it should then be possible to link loyalty_id to a single account number, in practice, there are a significant number of records that are lost in the process. 
In specific, there are instances where there are multiple different pi_entity linked to the same loyalty_id and thus the same account number. Although
comparably it seems that roughly ~88% of the entity key base can be mapped 1-to-1, roughly ~12% of the remaining base is not and putliers experiencing 10+ links are present. Conversations with Tropical are ongoing however this is the current state of the linking given the data infrastructure. Workarounds
include filtering for the number of linked accounts by rank and a hard aggregation across account number although this is not a permanent solution and will be alot easier once nectar ids are added to the Argos transactions. 
