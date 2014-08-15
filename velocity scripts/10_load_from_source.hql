use gfora;


drop table if exists ${TARGET_TABLE};


SET hive.exec.parallel=true;
SET hive.stats.autogather=false;


SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;


create table gfora.${TARGET_TABLE}
ROW FORMAT DELIMITED
STORED AS SEQUENCEFILE
as
select
tr.*,


itms.PRODUCT_ITEM_COUNT,


itms.item[0].col1 as MODEL_IS_ARRANGED_BOOKING_VALUE_1,
itms.item[0].col2 as MODEL_AUTH_FAIL_COUNT_RISK_ID_LOOKUP_VALUE_1,
itms.item[0].col3 as CHECK_OUT_DATE_1,
itms.item[0].col4 as MODEL_PACKAGE_FLAG_1,
itms.item[0].col5 as MODEL_REFERRING_PARTNER_RISK_ID_LOOKUP_VALUE_1,
itms.item[0].col6 as MODEL_HOTEL_RATING_RISK_ID_LOOKUP_VALUE_1,
itms.item[0].col7 as MODEL_HOTEL_CITY_RISK_ID_LOOKUP_VALUE_1,
itms.item[0].col8 as MODEL_HOTEL_RISK_ID_LOOKUP_VALUE_1,
itms.item[0].col9 as MODEL_BOOKING_ITEM_PRICE_USD_VALUE_1,
itms.item[0].col10 as ITEM_USD_VALUE_1,
itms.item[0].col11 as ITEM_VENDOR_NAME_1,
itms.item[0].col12 as MODEL_SCORE_VALUE_1,
itms.item[0].col13 as CHECK_IN_DATE_1,
itms.item[0].col14 as ITEM_ACTIVITY_DESCRIPTION_1,
itms.item[0].col15 as ITEM_AIRLINES_1,
itms.item[0].col16 as ITEM_AIRPORT_COUNT_1,
itms.item[0].col17 as ITEM_AIRPORT_DROP_OFF_LOCATION_1,
itms.item[0].col18 as ITEM_AIRPORT_PICK_UP_LOCATION_1,
itms.item[0].col19 as ITEM_AIRPORTS_1,
itms.item[0].col20 as ITEM_CITY_1,
itms.item[0].col21 as ITEM_COUNT_1,
itms.item[0].col22 as ITEM_COUNTRY_1,
itms.item[0].col23 as ITEM_CURRENCY_CODE_1,
itms.item[0].col24 as ITEM_DAYS_UNTIL_DEPARTURE_1,
itms.item[0].col25 as ITEM_DAYS_UNTIL_PICK_UP_1,
itms.item[0].col26 as ITEM_DAYS_UNTIL_STAY_1,
itms.item[0].col27 as ITEM_DAYS_UNTIL_STAY_RISK_ID_1,
itms.item[0].col28 as ITEM_DAYS_UNTIL_USE_1,
itms.item[0].col29 as ITEM_DESTINATION_1,
itms.item[0].col30 as ITEM_DURATION_1,
itms.item[0].col31 as ITEM_HOTEL_ID_1,
itms.item[0].col32 as ITEM_ID_1,
itms.item[0].col33 as ITEM_IS_ETICKET_1,
itms.item[0].col34 as ITEM_IS_INTERNATIONAL_1,
itms.item[0].col35 as ITEM_IS_ONE_WAY_1,
itms.item[0].col36 as ITEM_IS_OPAQUE_HOTEL_1,
itms.item[0].col37 as ITEM_IS_PACKAGE_ITEM_1,
itms.item[0].col38 as ITEM_LENGTH_OF_STAY_1,
itms.item[0].col39 as ITEM_NAME_1,
itms.item[0].col40 as ITEM_POSTAL_CODE_1,
itms.item[0].col41 as ITEM_ROOM_COUNT_1,
itms.item[0].col42 as MODEL_PRODUCT_TYPE_NAME_1,
itms.item[0].col43 as PRODUCT_ITEM_COUNT_1,
itms.item[0].col44 as TRAVELER_IS_ANY_GIVEN_NAME_LOWER_CASE_1,
itms.item[0].col45 as TRAVELER_IS_ANY_GIVEN_NAME_UPPER_CASE_1,
itms.item[0].col46 as TRAVELER_IS_ANY_SURNAME_LOWER_CASE_1,
itms.item[0].col47 as TRAVELER_IS_ANY_SURNAME_UPPER_CASE_1,
itms.item[0].col48 as TRAVELER_PHONE_NUMBER_COUNT_1,
itms.item[0].col49 as TRAVELER_PHONE_NUMBER_LIST_1,
itms.item[0].col51 as MODEL_HIGHEST_IS_PAY_AREA_CODE_PRIM_TRAV_LOOKUP_VALUE_1,
itms.item[0].col52 as MODEL_HIGHEST_IS_PAY_AREA_CODE_ACCOUNT_LOOKUP_VALUE_1,
itms.item[0].col53 as MODEL_HIGHEST_IS_PAY_GIVEN_NAME_PRIM_TRAV_LOOKUP_VALUE_1,
itms.item[0].col54 as MODEL_HIGHEST_IS_PAY_LOCAL_NBR_ACCOUNT_LOOKUP_VALUE_1,
itms.item[0].col55 as MODEL_HIGHEST_IS_PAY_SURNAME_EMAIL_NAME_LOOKUP_VALUE_1,
itms.item[0].col56 as MODEL_IS_PRIM_TRAV_SURNAME_EMAIL_NAME_LOOKUP_VALUE_1,
itms.item[0].col57 as MODEL_HIGHEST_IS_PAY_STATE_PROV_IP_LOOKUP_VALUE_1,
itms.item[0].col58 as MODEL_PRIM_TRAV_PHONE_REPEATING_NBR_PCT_LOOKUP_VALUE_1,
itms.item[0].col59 as MODEL_HIGHEST_IS_PAYMENT_CITY_HOTEL_LOOKUP_VALUE_1,






itms.item[1].col1 as MODEL_IS_ARRANGED_BOOKING_VALUE_2,
itms.item[1].col2 as MODEL_AUTH_FAIL_COUNT_RISK_ID_LOOKUP_VALUE_2,
itms.item[1].col3 as CHECK_OUT_DATE_2,
itms.item[1].col4 as MODEL_PACKAGE_FLAG_2,
itms.item[1].col5 as MODEL_REFERRING_PARTNER_RISK_ID_LOOKUP_VALUE_2,
itms.item[1].col6 as MODEL_HOTEL_RATING_RISK_ID_LOOKUP_VALUE_2,
itms.item[1].col7 as MODEL_HOTEL_CITY_RISK_ID_LOOKUP_VALUE_2,
itms.item[1].col8 as MODEL_HOTEL_RISK_ID_LOOKUP_VALUE_2,
itms.item[1].col9 as MODEL_BOOKING_ITEM_PRICE_USD_VALUE_2,
itms.item[1].col10 as ITEM_USD_VALUE_2,
itms.item[1].col11 as ITEM_VENDOR_NAME_2,
itms.item[1].col12 as MODEL_SCORE_VALUE_2,
itms.item[1].col13 as CHECK_IN_DATE_2,
itms.item[1].col14 as ITEM_ACTIVITY_DESCRIPTION_2,
itms.item[1].col15 as ITEM_AIRLINES_2,
itms.item[1].col16 as ITEM_AIRPORT_COUNT_2,
itms.item[1].col17 as ITEM_AIRPORT_DROP_OFF_LOCATION_2,
itms.item[1].col18 as ITEM_AIRPORT_PICK_UP_LOCATION_2,
itms.item[1].col19 as ITEM_AIRPORTS_2,
itms.item[1].col20 as ITEM_CITY_2,
itms.item[1].col21 as ITEM_COUNT_2,
itms.item[1].col22 as ITEM_COUNTRY_2,
itms.item[1].col23 as ITEM_CURRENCY_CODE_2,
itms.item[1].col24 as ITEM_DAYS_UNTIL_DEPARTURE_2,
itms.item[1].col25 as ITEM_DAYS_UNTIL_PICK_UP_2,
itms.item[1].col26 as ITEM_DAYS_UNTIL_STAY_2,
itms.item[1].col27 as ITEM_DAYS_UNTIL_STAY_RISK_ID_2,
itms.item[1].col28 as ITEM_DAYS_UNTIL_USE_2,
itms.item[1].col29 as ITEM_DESTINATION_2,
itms.item[1].col30 as ITEM_DURATION_2,
itms.item[1].col31 as ITEM_HOTEL_ID_2,
itms.item[1].col32 as ITEM_ID_2,
itms.item[1].col33 as ITEM_IS_ETICKET_2,
itms.item[1].col34 as ITEM_IS_INTERNATIONAL_2,
itms.item[1].col35 as ITEM_IS_ONE_WAY_2,
itms.item[1].col36 as ITEM_IS_OPAQUE_HOTEL_2,
itms.item[1].col37 as ITEM_IS_PACKAGE_ITEM_2,
itms.item[1].col38 as ITEM_LENGTH_OF_STAY_2,
itms.item[1].col39 as ITEM_NAME_2,
itms.item[1].col40 as ITEM_POSTAL_CODE_2,
itms.item[1].col41 as ITEM_ROOM_COUNT_2,
itms.item[1].col42 as MODEL_PRODUCT_TYPE_NAME_2,
itms.item[1].col43 as PRODUCT_ITEM_COUNT_2,
itms.item[1].col44 as TRAVELER_IS_ANY_GIVEN_NAME_LOWER_CASE_2,
itms.item[1].col45 as TRAVELER_IS_ANY_GIVEN_NAME_UPPER_CASE_2,
itms.item[1].col46 as TRAVELER_IS_ANY_SURNAME_LOWER_CASE_2,
itms.item[1].col47 as TRAVELER_IS_ANY_SURNAME_UPPER_CASE_2,
itms.item[1].col48 as TRAVELER_PHONE_NUMBER_COUNT_2,
itms.item[1].col49 as TRAVELER_PHONE_NUMBER_LIST_2,
itms.item[1].col51 as MODEL_HIGHEST_IS_PAY_AREA_CODE_PRIM_TRAV_LOOKUP_VALUE_2,
itms.item[1].col52 as MODEL_HIGHEST_IS_PAY_AREA_CODE_ACCOUNT_LOOKUP_VALUE_2,
itms.item[1].col53 as MODEL_HIGHEST_IS_PAY_GIVEN_NAME_PRIM_TRAV_LOOKUP_VALUE_2,
itms.item[1].col54 as MODEL_HIGHEST_IS_PAY_LOCAL_NBR_ACCOUNT_LOOKUP_VALUE_2,
itms.item[1].col55 as MODEL_HIGHEST_IS_PAY_SURNAME_EMAIL_NAME_LOOKUP_VALUE_2,
itms.item[1].col56 as MODEL_IS_PRIM_TRAV_SURNAME_EMAIL_NAME_LOOKUP_VALUE_2,
itms.item[1].col57 as MODEL_HIGHEST_IS_PAY_STATE_PROV_IP_LOOKUP_VALUE_2,
itms.item[1].col58 as MODEL_PRIM_TRAV_PHONE_REPEATING_NBR_PCT_LOOKUP_VALUE_2,
itms.item[1].col59 as MODEL_HIGHEST_IS_PAYMENT_CITY_HOTEL_LOOKUP_VALUE_2,


itms.item[2].col1 as  MODEL_IS_ARRANGED_BOOKING_VALUE_3,
itms.item[2].col2 as  MODEL_AUTH_FAIL_COUNT_RISK_ID_LOOKUP_VALUE_3,
itms.item[2].col3 as  CHECK_OUT_DATE_3,
itms.item[2].col4 as  MODEL_PACKAGE_FLAG_3,
itms.item[2].col5 as  MODEL_REFERRING_PARTNER_RISK_ID_LOOKUP_VALUE_3,
itms.item[2].col6 as  MODEL_HOTEL_RATING_RISK_ID_LOOKUP_VALUE_3,
itms.item[2].col7 as  MODEL_HOTEL_CITY_RISK_ID_LOOKUP_VALUE_3,
itms.item[2].col8 as  MODEL_HOTEL_RISK_ID_LOOKUP_VALUE_3,
itms.item[2].col9 as  MODEL_BOOKING_ITEM_PRICE_USD_VALUE_3,
itms.item[2].col10 as ITEM_USD_VALUE_3,
itms.item[2].col11 as ITEM_VENDOR_NAME_3,
itms.item[2].col12 as MODEL_SCORE_VALUE_3,
itms.item[2].col13 as CHECK_IN_DATE_3,
itms.item[2].col14 as ITEM_ACTIVITY_DESCRIPTION_3,
itms.item[2].col15 as ITEM_AIRLINES_3,
itms.item[2].col16 as ITEM_AIRPORT_COUNT_3,
itms.item[2].col17 as ITEM_AIRPORT_DROP_OFF_LOCATION_3,
itms.item[2].col18 as ITEM_AIRPORT_PICK_UP_LOCATION_3,
itms.item[2].col19 as ITEM_AIRPORTS_3,
itms.item[2].col20 as ITEM_CITY_3,
itms.item[2].col21 as ITEM_COUNT_3,
itms.item[2].col22 as ITEM_COUNTRY_3,
itms.item[2].col23 as ITEM_CURRENCY_CODE_3,
itms.item[2].col24 as ITEM_DAYS_UNTIL_DEPARTURE_3,
itms.item[2].col25 as ITEM_DAYS_UNTIL_PICK_UP_3,
itms.item[2].col26 as ITEM_DAYS_UNTIL_STAY_3,
itms.item[2].col27 as ITEM_DAYS_UNTIL_STAY_RISK_ID_3,
itms.item[2].col28 as ITEM_DAYS_UNTIL_USE_3,
itms.item[2].col29 as ITEM_DESTINATION_3,
itms.item[2].col30 as ITEM_DURATION_3,
itms.item[2].col31 as ITEM_HOTEL_ID_3,
itms.item[2].col32 as ITEM_ID_3,
itms.item[2].col33 as ITEM_IS_ETICKET_3,
itms.item[2].col34 as ITEM_IS_INTERNATIONAL_3,
itms.item[2].col35 as ITEM_IS_ONE_WAY_3,
itms.item[2].col36 as ITEM_IS_OPAQUE_HOTEL_3,
itms.item[2].col37 as ITEM_IS_PACKAGE_ITEM_3,
itms.item[2].col38 as ITEM_LENGTH_OF_STAY_3,
itms.item[2].col39 as ITEM_NAME_3,
itms.item[2].col40 as ITEM_POSTAL_CODE_3,
itms.item[2].col41 as ITEM_ROOM_COUNT_3,
itms.item[2].col42 as MODEL_PRODUCT_TYPE_NAME_3,
itms.item[2].col43 as PRODUCT_ITEM_COUNT_3,
itms.item[2].col44 as TRAVELER_IS_ANY_GIVEN_NAME_LOWER_CASE_3,
itms.item[2].col45 as TRAVELER_IS_ANY_GIVEN_NAME_UPPER_CASE_3,
itms.item[2].col46 as TRAVELER_IS_ANY_SURNAME_LOWER_CASE_3,
itms.item[2].col47 as TRAVELER_IS_ANY_SURNAME_UPPER_CASE_3,
itms.item[2].col48 as TRAVELER_PHONE_NUMBER_COUNT_3,
itms.item[2].col49 as TRAVELER_PHONE_NUMBER_LIST_3,                        
itms.item[2].col51 as MODEL_HIGHEST_IS_PAY_AREA_CODE_PRIM_TRAV_LOOKUP_VALUE_3,
itms.item[2].col52 as MODEL_HIGHEST_IS_PAY_AREA_CODE_ACCOUNT_LOOKUP_VALUE_3,
itms.item[2].col53 as MODEL_HIGHEST_IS_PAY_GIVEN_NAME_PRIM_TRAV_LOOKUP_VALUE_3,
itms.item[2].col54 as MODEL_HIGHEST_IS_PAY_LOCAL_NBR_ACCOUNT_LOOKUP_VALUE_3,
itms.item[2].col55 as MODEL_HIGHEST_IS_PAY_SURNAME_EMAIL_NAME_LOOKUP_VALUE_3,
itms.item[2].col56 as MODEL_IS_PRIM_TRAV_SURNAME_EMAIL_NAME_LOOKUP_VALUE_3,
itms.item[2].col57 as MODEL_HIGHEST_IS_PAY_STATE_PROV_IP_LOOKUP_VALUE_3,
itms.item[2].col58 as MODEL_PRIM_TRAV_PHONE_REPEATING_NBR_PCT_LOOKUP_VALUE_3,
itms.item[2].col59 as MODEL_HIGHEST_IS_PAYMENT_CITY_HOTEL_LOOKUP_VALUE_3,


trvs.traveler[0].col1 as TRAVELER_GIVEN_NAME_1,
trvs.traveler[0].col2 as TRAVELER_MIDDLE_NAME_1,
trvs.traveler[0].col3 as TRAVELER_LAST_NAME_1,
trvs.traveler[0].col4 as TRAVELER_GENDER_NAME_1,
trvs.traveler[0].col5 as TRAVELER_HOME_PHONE_AREA_CODE_1,
trvs.traveler[0].col6 as TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_1, 
trvs.traveler[0].col7 as TRAVELER_HOME_PHONE_NUMBER_1,
trvs.traveler[0].col8 as TRAVELER_BUSINESS_PHONE_AREA_CODE_1,
trvs.traveler[0].col9 as TRAVELER_BUSINESS_PHONE_COUNTRY_ACCESS_CODE_1,
trvs.traveler[0].col10 as traveler_business_phone_number_1,
trvs.traveler[0].col11 as traveler_mobile_phone_area_code_1,
trvs.traveler[0].col12 as traveler_mobile_phone_country_access_code_1,
trvs.traveler[0].col13 as traveler_mobile_phone_number_1,
trvs.traveler[0].col14 as traveler_email_address_1,
trvs.traveler[0].col15 as traveler_is_given_name_lower_case_1,
trvs.traveler[0].col16 as traveler_is_given_name_upper_case_1,
trvs.traveler[0].col17 as traveler_is_primary_1,
trvs.traveler[0].col18 as traveler_is_surname_lower_case_1,
trvs.traveler[0].col19 as traveler_is_surname_upper_case_1,
trvs.traveler[0].col20 as traveler_full_name_1,




trvs.traveler[1].col1 as TRAVELER_GIVEN_NAME_2,
trvs.traveler[1].col2 as TRAVELER_MIDDLE_NAME_2,
trvs.traveler[1].col3 as TRAVELER_LAST_NAME_2,
trvs.traveler[1].col4 as TRAVELER_GENDER_NAME_2,
trvs.traveler[1].col5 as TRAVELER_HOME_PHONE_AREA_CODE_2,
trvs.traveler[1].col6 as TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_2, 
trvs.traveler[1].col7 as TRAVELER_HOME_PHONE_NUMBER_2,
trvs.traveler[1].col8 as TRAVELER_BUSINESS_PHONE_AREA_CODE_2,
trvs.traveler[1].col9 as TRAVELER_BUSINESS_PHONE_COUNTRY_ACCESS_CODE_2,
trvs.traveler[1].col10 as traveler_business_phone_number_2,
trvs.traveler[1].col11 as traveler_mobile_phone_area_code_2,
trvs.traveler[1].col12 as traveler_mobile_phone_country_access_code_2,
trvs.traveler[1].col13 as traveler_mobile_phone_number_2,
trvs.traveler[1].col14 as traveler_email_address_2,
trvs.traveler[1].col15 as traveler_is_given_name_lower_case_2,
trvs.traveler[1].col16 as traveler_is_given_name_upper_case_2,
trvs.traveler[1].col17 as traveler_is_primary_2,
trvs.traveler[1].col18 as traveler_is_surname_lower_case_2,
trvs.traveler[1].col19 as traveler_is_surname_upper_case_2,
trvs.traveler[1].col20 as traveler_full_name_2,


trvs.traveler[2].col1 as TRAVELER_GIVEN_NAME_3,
trvs.traveler[2].col2 as TRAVELER_MIDDLE_NAME_3,
trvs.traveler[2].col3 as TRAVELER_LAST_NAME_3,
trvs.traveler[2].col4 as TRAVELER_GENDER_NAME_3,
trvs.traveler[2].col5 as TRAVELER_HOME_PHONE_AREA_CODE_3,
trvs.traveler[2].col6 as TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_3, 
trvs.traveler[2].col7 as TRAVELER_HOME_PHONE_NUMBER_3,
trvs.traveler[2].col8 as TRAVELER_BUSINESS_PHONE_AREA_CODE_3,
trvs.traveler[2].col9 as TRAVELER_BUSINESS_PHONE_COUNTRY_ACCESS_CODE_3,
trvs.traveler[2].col10 as traveler_business_phone_number_3,
trvs.traveler[2].col11 as traveler_mobile_phone_area_code_3,
trvs.traveler[2].col12 as traveler_mobile_phone_country_access_code_3,
trvs.traveler[2].col13 as traveler_mobile_phone_number_3,
trvs.traveler[2].col14 as traveler_email_address_3,
trvs.traveler[2].col15 as traveler_is_given_name_lower_case_3,
trvs.traveler[2].col16 as traveler_is_given_name_upper_case_3,
trvs.traveler[2].col17 as traveler_is_primary_3,
trvs.traveler[2].col18 as traveler_is_surname_lower_case_3,
trvs.traveler[2].col19 as traveler_is_surname_upper_case_3,
trvs.traveler[2].col20 as traveler_full_name_3,


trvs.traveler[3].col1 as TRAVELER_GIVEN_NAME_4,
trvs.traveler[3].col2 as TRAVELER_MIDDLE_NAME_4,
trvs.traveler[3].col3 as TRAVELER_LAST_NAME_4,
trvs.traveler[3].col4 as TRAVELER_GENDER_NAME_4,
trvs.traveler[3].col5 as TRAVELER_HOME_PHONE_AREA_CODE_4,
trvs.traveler[3].col6 as TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_4, 
trvs.traveler[3].col7 as TRAVELER_HOME_PHONE_NUMBER_4,
trvs.traveler[3].col8 as TRAVELER_BUSINESS_PHONE_AREA_CODE_4,
trvs.traveler[3].col9 as TRAVELER_BUSINESS_PHONE_COUNTRY_ACCESS_CODE_4,
trvs.traveler[3].col10 as traveler_business_phone_number_4,
trvs.traveler[3].col11 as traveler_mobile_phone_area_code_4,
trvs.traveler[3].col12 as traveler_mobile_phone_country_access_code_4,
trvs.traveler[3].col13 as traveler_mobile_phone_number_4,
trvs.traveler[3].col14 as traveler_email_address_4,
trvs.traveler[3].col15 as traveler_is_given_name_lower_case_4,
trvs.traveler[3].col16 as traveler_is_given_name_upper_case_4,
trvs.traveler[3].col17 as traveler_is_primary_4,
trvs.traveler[3].col18 as traveler_is_surname_lower_case_4,
trvs.traveler[3].col19 as traveler_is_surname_upper_case_4,
trvs.traveler[3].col20 as traveler_full_name_4,


trvs.traveler[4].col1 as TRAVELER_GIVEN_NAME_5,
trvs.traveler[4].col2 as TRAVELER_MIDDLE_NAME_5,
trvs.traveler[4].col3 as TRAVELER_LAST_NAME_5,
trvs.traveler[4].col4 as TRAVELER_GENDER_NAME_5,
trvs.traveler[4].col5 as TRAVELER_HOME_PHONE_AREA_CODE_5,
trvs.traveler[4].col6 as TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_5, 
trvs.traveler[4].col7 as TRAVELER_HOME_PHONE_NUMBER_5,
trvs.traveler[4].col8 as TRAVELER_BUSINESS_PHONE_AREA_CODE_5,
trvs.traveler[4].col9 as TRAVELER_BUSINESS_PHONE_COUNTRY_ACCESS_CODE_5,
trvs.traveler[4].col10 as traveler_business_phone_number_5,
trvs.traveler[4].col11 as traveler_mobile_phone_area_code_5,
trvs.traveler[4].col12 as traveler_mobile_phone_country_access_code_5,
trvs.traveler[4].col13 as traveler_mobile_phone_number_5,
trvs.traveler[4].col14 as traveler_email_address_5,
trvs.traveler[4].col15 as traveler_is_given_name_lower_case_5,
trvs.traveler[4].col16 as traveler_is_given_name_upper_case_5,
trvs.traveler[4].col17 as traveler_is_primary_5,
trvs.traveler[4].col18 as traveler_is_surname_lower_case_5,
trvs.traveler[4].col19 as traveler_is_surname_upper_case_5,
trvs.traveler[4].col20 as traveler_full_name_5,


items1.item[0].col1 as ITEM_SEGMENT_AIRLINE_1, 
items1.item[0].col2 as ITEM_SEGMENT_ARRIVAL_AIRPORT_1, 
items1.item[0].col3 as ITEM_SEGMENT_DEPARTURE_AIRPORT_1, 
items1.item[0].col4 as ITEM_SEGMENT_IS_DESTINATION_1,
items1.item[0].col5 as ITEM_SEGMENT_IS_ORIGIN_1,


items1.item[1].col1 as ITEM_SEGMENT_AIRLINE_2, 
items1.item[1].col2 as ITEM_SEGMENT_ARRIVAL_AIRPORT_2, 
items1.item[1].col3 as ITEM_SEGMENT_DEPARTURE_AIRPORT_2, 
items1.item[1].col4 as ITEM_SEGMENT_IS_DESTINATION_2,
items1.item[1].col5 as ITEM_SEGMENT_IS_ORIGIN_2,


items1.item[2].col1 as ITEM_SEGMENT_AIRLINE_3, 
items1.item[2].col2 as ITEM_SEGMENT_ARRIVAL_AIRPORT_3, 
items1.item[2].col3 as ITEM_SEGMENT_DEPARTURE_AIRPORT_3, 
items1.item[2].col4 as ITEM_SEGMENT_IS_DESTINATION_3,
items1.item[2].col5 as ITEM_SEGMENT_IS_ORIGIN_3,


items1.item[3].col1 as ITEM_SEGMENT_AIRLINE_4, 
items1.item[3].col2 as ITEM_SEGMENT_ARRIVAL_AIRPORT_4, 
items1.item[3].col3 as ITEM_SEGMENT_DEPARTURE_AIRPORT_4, 
items1.item[3].col4 as ITEM_SEGMENT_IS_DESTINATION_4,
items1.item[3].col5 as ITEM_SEGMENT_IS_ORIGIN_4,


items1.item[4].col1 as ITEM_SEGMENT_AIRLINE_5, 
items1.item[4].col2 as ITEM_SEGMENT_ARRIVAL_AIRPORT_5, 
items1.item[4].col3 as ITEM_SEGMENT_DEPARTURE_AIRPORT_5, 
items1.item[4].col4 as ITEM_SEGMENT_IS_DESTINATION_5,
items1.item[4].col5 as ITEM_SEGMENT_IS_ORIGIN_5,


pmts.PAYMENT_ITEM_COUNT,
pmts.payment[0].col1 as FORM_OF_PAYMENT_TYPE_1,
pmts.payment[0].col2 as PAYMENT_ADDRESS_LINE_1_1,
pmts.payment[0].col3 as PAYMENT_AMOUNT_CURRENCY_CODE_1,
pmts.payment[0].col4 as PAYMENT_ANY_TRAVELER_SURNAME_NOT_MATCH_1,
pmts.payment[0].col5 as PAYMENT_AREA_CODE_1,
pmts.payment[0].col6 as PAYMENT_AVS_RESPONSE_CODE_1,
pmts.payment[0].col7 as PAYMENT_BIN_NUMBER_1,
pmts.payment[0].col8 as PAYMENT_CITY_1,
pmts.payment[0].col9 as PAYMENT_COMPANY_NAME_1,
pmts.payment[0].col10 as PAYMENT_COUNTRY_1,
pmts.payment[0].col11 as PAYMENT_COUNTRY_ACCESS_CODE_1,
pmts.payment[0].col12 as PAYMENT_EXPIRATION_DATE_1,
pmts.payment[0].col13 as PAYMENT_GIVEN_NAME_1,
pmts.payment[0].col14 as PAYMENT_IS_ANY_TRAVELER_SURNAME_1,
pmts.payment[0].col15 as PAYMENT_IS_GIVEN_NAME_UPPER_CASE_1,
pmts.payment[0].col16 as PAYMENT_IS_PRIMARY_TRAVELER_GIVEN_NAME_1,
pmts.payment[0].col17 as PAYMENT_IS_PRIMARY_TRAVELER_SURNAME_1,
pmts.payment[0].col18 as PAYMENT_IS_SURNAME_ANY_TRAV_OR_ACCT_1,
pmts.payment[0].col19 as PAYMENT_IS_SURNAME_UPPER_CASE_1,
pmts.payment[0].col20 as PAYMENT_ISSUE_COUNTRY_1,
pmts.payment[0].col21 as PAYMENT_LAST_NAME_1,
pmts.payment[0].col22 as PAYMENT_POSTAL_CODE_1,
pmts.payment[0].col23 as PAYMENT_STATE_PROV_1,
pmts.payment[0].col24 as PAYMENT_USD_AMOUNT_1,
pmts.payment[0].col25 as PAYMENT_AUTH_RESPONSE_CODE_1,
pmts.payment[0].col26 as PAYMENT_AUTH_RESPONSE_GROUP_1,
pmts.payment[0].col27 as PAYMENT_CSV_CHECK_STATUS_1,
pmts.payment[0].col28 as PAYMENT_EMAIL_ADDRESS_1,
pmts.payment[0].col29 as PAYMENT_MIDDLE_NAME_1,
pmts.payment[0].col30 as PAYMENT_PAYER_ID_1,
pmts.payment[0].col31 as PAYMENT_PHONE_NUMBER_1,
pmts.payment[0].col32 as PAYMENT_PHONE_TYPE_1,
pmts.payment[0].col33 as PAYMENT_ADDRESS_LINE_2_1,
pmts.payment[0].col34 as PAYMENT_ADDRESS_LINE_3_1,
pmts.payment[0].col35 as PAYMENT_ADDRESS_LINE_4_1,
pmts.payment[0].col36 as PAYMENT_LOCAL_AMOUNT_1,
pmts.payment[0].col37 as PAYMENT_LAST_NAME_DERIVED_1,


pmts.payment[1].col1 as FORM_OF_PAYMENT_TYPE_2,
pmts.payment[1].col2 as PAYMENT_ADDRESS_LINE_1_2,
pmts.payment[1].col3 as PAYMENT_AMOUNT_CURRENCY_CODE_2,
pmts.payment[1].col4 as PAYMENT_ANY_TRAVELER_SURNAME_NOT_MATCH_2,
pmts.payment[1].col5 as PAYMENT_AREA_CODE_2,
pmts.payment[1].col6 as PAYMENT_AVS_RESPONSE_CODE_2,
pmts.payment[1].col7 as PAYMENT_BIN_NUMBER_2,
pmts.payment[1].col8 as PAYMENT_CITY_2,
pmts.payment[1].col9 as PAYMENT_COMPANY_NAME_2,
pmts.payment[1].col10 as PAYMENT_COUNTRY_2,
pmts.payment[1].col11 as PAYMENT_COUNTRY_ACCESS_CODE_2,
pmts.payment[1].col12 as PAYMENT_EXPIRATION_DATE_2,
pmts.payment[1].col13 as PAYMENT_GIVEN_NAME_2,
pmts.payment[1].col14 as PAYMENT_IS_ANY_TRAVELER_SURNAME_2,
pmts.payment[1].col15 as PAYMENT_IS_GIVEN_NAME_UPPER_CASE_2,
pmts.payment[1].col16 as PAYMENT_IS_PRIMARY_TRAVELER_GIVEN_NAME_2,
pmts.payment[1].col17 as PAYMENT_IS_PRIMARY_TRAVELER_SURNAME_2,
pmts.payment[1].col18 as PAYMENT_IS_SURNAME_ANY_TRAV_OR_ACCT_2,
pmts.payment[1].col19 as PAYMENT_IS_SURNAME_UPPER_CASE_2,
pmts.payment[1].col20 as PAYMENT_ISSUE_COUNTRY_2,
pmts.payment[1].col21 as PAYMENT_LAST_NAME_2,
pmts.payment[1].col22 as PAYMENT_POSTAL_CODE_2,
pmts.payment[1].col23 as PAYMENT_STATE_PROV_2,
pmts.payment[1].col24 as PAYMENT_USD_AMOUNT_2,
pmts.payment[1].col25 as PAYMENT_AUTH_RESPONSE_CODE_2,
pmts.payment[1].col26 as PAYMENT_AUTH_RESPONSE_GROUP_2,
pmts.payment[1].col27 as PAYMENT_CSV_CHECK_STATUS_2,
pmts.payment[1].col28 as PAYMENT_EMAIL_ADDRESS_2,
pmts.payment[1].col29 as PAYMENT_MIDDLE_NAME_2,
pmts.payment[1].col30 as PAYMENT_PAYER_ID_2,
pmts.payment[1].col31 as PAYMENT_PHONE_NUMBER_2,
pmts.payment[1].col32 as PAYMENT_PHONE_TYPE_2,
pmts.payment[1].col33 as PAYMENT_ADDRESS_LINE_2_2,
pmts.payment[1].col34 as PAYMENT_ADDRESS_LINE_3_2,
pmts.payment[1].col35 as PAYMENT_ADDRESS_LINE_4_2,
pmts.payment[1].col36 as PAYMENT_LOCAL_AMOUNT_2,
pmts.payment[1].col37 as PAYMENT_LAST_NAME_DERIVED_2,


oms1.accountnum_bex,
oms1.accountnum_h,	
oms1.accountnum_ean,
oms1.loyaltyid_bex,	
oms1.loyaltyid_h,
oms1.loyaltyid_ean,	
oms1.accountphonenum_h,
oms1.currency as frauddata_001_currency,
oms1.devicetype_bex,
oms1.country_bex,
oms1.country_h,


oms2.external_client_id,
oms2.accountnum,	
oms2.loyaltyid,	
oms2.accountphonenum,	
oms2.referrersource,	
oms2.currency as frauddata_002_currency,	
oms2.devicetype,
oms2.browserlanguage,
oms2.websiteurl,
oms2.country,
oms2.eapid,
oms2.affiliateid,
oms2.transactiondatetime,


oms3.values[0].col1 as order_line_id_1,
oms3.values[0].col2 as order_line_type_id_1,
oms3.values[0].col3 as booking_record_id_1,
oms3.values[0].col4 as booking_item_id_1,
oms3.values[0].col5 as tsatravelid_1,
oms3.values[0].col6 as frequentflyernumber_1,
oms3.values[0].col7 as passportnumber_1,
oms3.values[0].col8 as vendorcode_1,
oms3.values[0].col9 as inventorytype_1,
oms3.values[0].col10 as travelingwithchild_1,
oms3.values[0].col11 as refundability_1,


oms3.values[1].col1 as order_line_id_2,
oms3.values[1].col2 as order_line_type_id_2,
oms3.values[1].col3 as booking_record_id_2,
oms3.values[1].col4 as booking_item_id_2,
oms3.values[1].col5 as tsatravelid_2,
oms3.values[1].col6 as frequentflyernumber_2,
oms3.values[1].col7 as passportnumber_2,
oms3.values[1].col8 as vendorcode_2,
oms3.values[1].col9 as inventorytype_2,
oms3.values[1].col10 as travelingwithchild_2,
oms3.values[1].col11 as refundability_2,


flat2.cards[0].col1 as PAYMENT_HASHED_CREDIT_CARD_NUMBER_1,
flat2.cards[0].col2 as PAYMENT_CREDIT_CARD_EXPIRATION_DATE_1,


flat2.cards[1].col1 as PAYMENT_HASHED_CREDIT_CARD_NUMBER_2,
flat2.cards[1].col2 as PAYMENT_CREDIT_CARD_EXPIRATION_DATE_2,


unq_travs.unique_travelers_count,


unq_travs.unique_travelers_list[0] as UNIQUE_TRAVELER_NAME_1,
unq_travs.unique_travelers_list[1] as UNIQUE_TRAVELER_NAME_2,
unq_travs.unique_travelers_list[2] as UNIQUE_TRAVELER_NAME_3,
unq_travs.unique_travelers_list[3] as UNIQUE_TRAVELER_NAME_4,
unq_travs.unique_travelers_list[4] as UNIQUE_TRAVELER_NAME_5,


outcome.transactiondate,
outcome.iscompletedbooking,
outcome.statusid,
outcome.outcomeid,
case outcome.outcomeid 
    when -1 then 'Incomplete'
    when 0 then 'Unknown'
    when 1 then 'FraudCaught'
    when 2 then 'FraudMissed'
    when 3 then 'Insult'
    when 4 then 'Good'
    else 'Totally Unknown!'
end as Outcome_Name,
substr(tr.CLIENT_INPUT_STREAM_ID,14) as TPID_DERIVED,
concat_ws(' ', account_home_phone_country_access_code, account_home_phone_area_code, account_home_phone_number) as ACCOUNT_HOME_PHONE_DERIVED
 




from etldata.fraud_scoring_xml_detail tr


left outer join (
    select 
        fraud_transaction_id,
        max(PRODUCT_ITEM_COUNT) as PRODUCT_ITEM_COUNT,
        exp_collect_struct(
            struct(
                MODEL_IS_ARRANGED_BOOKING_VALUE,
                MODEL_AUTH_FAIL_COUNT_RISK_ID_LOOKUP_VALUE,
                CHECK_OUT_DATE,
                MODEL_PACKAGE_FLAG,
                MODEL_REFERRING_PARTNER_RISK_ID_LOOKUP_VALUE,
                MODEL_HOTEL_RATING_RISK_ID_LOOKUP_VALUE,
                MODEL_HOTEL_CITY_RISK_ID_LOOKUP_VALUE,
                MODEL_HOTEL_RISK_ID_LOOKUP_VALUE,
                MODEL_BOOKING_ITEM_PRICE_USD_VALUE,
                ITEM_USD_VALUE,
                ITEM_VENDOR_NAME,
                MODEL_SCORE_VALUE,
                CHECK_IN_DATE,
                ITEM_ACTIVITY_DESCRIPTION,
                ITEM_AIRLINES,
                ITEM_AIRPORT_COUNT,
                ITEM_AIRPORT_DROP_OFF_LOCATION,
                ITEM_AIRPORT_PICK_UP_LOCATION,
                ITEM_AIRPORTS,
                ITEM_CITY,
                ITEM_COUNT,
                ITEM_COUNTRY,
                ITEM_CURRENCY_CODE,
                ITEM_DAYS_UNTIL_DEPARTURE,
                ITEM_DAYS_UNTIL_PICK_UP,
                ITEM_DAYS_UNTIL_STAY,
                ITEM_DAYS_UNTIL_STAY_RISK_ID,
                ITEM_DAYS_UNTIL_USE,
                ITEM_DESTINATION,
                ITEM_DURATION,
                ITEM_HOTEL_ID,
                ITEM_ID,
                ITEM_IS_ETICKET,
                ITEM_IS_INTERNATIONAL,
                ITEM_IS_ONE_WAY,
                ITEM_IS_OPAQUE_HOTEL,
                ITEM_IS_PACKAGE_ITEM,
                ITEM_LENGTH_OF_STAY,
                ITEM_NAME,
                ITEM_POSTAL_CODE,
                ITEM_ROOM_COUNT,
                MODEL_PRODUCT_TYPE_NAME,
                PRODUCT_ITEM_COUNT,
                TRAVELER_IS_ANY_GIVEN_NAME_LOWER_CASE,
                TRAVELER_IS_ANY_GIVEN_NAME_UPPER_CASE,
                TRAVELER_IS_ANY_SURNAME_LOWER_CASE,
                TRAVELER_IS_ANY_SURNAME_UPPER_CASE,
                TRAVELER_PHONE_NUMBER_COUNT,
                TRAVELER_PHONE_NUMBER_LIST,
                if (ITEM_USD_VALUE <> '', ITEM_USD_VALUE, MODEL_BOOKING_ITEM_PRICE_USD_RISK_ID_LOOKUP_VALUE),
                MODEL_HIGHEST_IS_PAY_AREA_CODE_PRIM_TRAV_LOOKUP_VALUE,
                MODEL_HIGHEST_IS_PAY_AREA_CODE_ACCOUNT_LOOKUP_VALUE,
                MODEL_HIGHEST_IS_PAY_GIVEN_NAME_PRIM_TRAV_LOOKUP_VALUE,
                MODEL_HIGHEST_IS_PAY_LOCAL_NBR_ACCOUNT_LOOKUP_VALUE,
                MODEL_HIGHEST_IS_PAY_SURNAME_EMAIL_NAME_LOOKUP_VALUE,
                MODEL_IS_PRIM_TRAV_SURNAME_EMAIL_NAME_LOOKUP_VALUE,
                MODEL_HIGHEST_IS_PAY_STATE_PROV_IP_LOOKUP_VALUE,
                MODEL_PRIM_TRAV_PHONE_REPEATING_NBR_PCT_LOOKUP_VALUE,
                MODEL_HIGHEST_IS_PAYMENT_CITY_HOTEL_LOOKUP_VALUE


            )
        ) as item
    from etldata.fraud_scoring_xml_model_detail
    where 
        fraud_transaction_create_date between '${START_DATE}' and '${END_DATE}'
    group by fraud_transaction_id
) itms on itms.fraud_transaction_id = tr.fraud_transaction_id


left outer join (
    select 
        fraud_transaction_id,
        exp_collect_struct(
            struct(
                trv_list.col1,
                trv_list.col2,
                trv_list.col3,
                trv_list.col4,
                trv_list.col5,
                trv_list.col6,
                trv_list.col7,
                trv_list.col8,
                trv_list.col9,
                trv_list.col10,
                trv_list.col11,
                trv_list.col12,
                trv_list.col13,
                trv_list.col14,
                trv_list.col15,
                trv_list.col16,
                trv_list.col17,
                trv_list.col18,
                trv_list.col19,
                concat_ws('_*',trv_list.col1,trv_list.col2,trv_list.col3)
            )
        ) as traveler
    from etldata.fraud_scoring_xml_model_detail lateral view explode(
        array(
            struct(TRAVELER_GIVEN_NAME_1, TRAVELER_MIDDLE_NAME_1, TRAVELER_LAST_NAME_1, TRAVELER_GENDER_NAME_1,
            TRAVELER_1_HOME_PHONE_AREA_CODE,TRAVELER_1_HOME_PHONE_COUNTRY_ACCESS_CODE, TRAVELER_1_HOME_PHONE_NUMBER,
            TRAVELER_1_BUSINESS_PHONE_AREA_CODE,TRAVELER_1_BUSINESS_PHONE_COUNTRY_ACCESS_CODE,
            traveler_1_business_phone_number,traveler_1_mobile_phone_area_code,traveler_1_mobile_phone_country_access_code,
            traveler_1_mobile_phone_number,traveler_1_email_address,
            traveler_is_given_name_lower_case_1,traveler_is_given_name_upper_case_1,traveler_is_primary_1,
            traveler_is_surname_lower_case_1,traveler_is_surname_upper_case_1
            ),
            struct(TRAVELER_GIVEN_NAME_2, TRAVELER_MIDDLE_NAME_2, TRAVELER_LAST_NAME_2, TRAVELER_GENDER_NAME_2,
            TRAVELER_2_HOME_PHONE_AREA_CODE,TRAVELER_2_HOME_PHONE_COUNTRY_ACCESS_CODE, TRAVELER_2_HOME_PHONE_NUMBER,
            TRAVELER_2_BUSINESS_PHONE_AREA_CODE,TRAVELER_2_BUSINESS_PHONE_COUNTRY_ACCESS_CODE,
            traveler_2_business_phone_number,traveler_2_mobile_phone_area_code,traveler_2_mobile_phone_country_access_code,
            traveler_2_mobile_phone_number,traveler_2_email_address,
            traveler_is_given_name_lower_case_2,traveler_is_given_name_upper_case_2,traveler_is_primary_2,
            traveler_is_surname_lower_case_2,traveler_is_surname_upper_case_2
            ),
            struct(TRAVELER_GIVEN_NAME_3, TRAVELER_MIDDLE_NAME_3, TRAVELER_LAST_NAME_3, TRAVELER_GENDER_NAME_3,
            TRAVELER_3_HOME_PHONE_AREA_CODE,TRAVELER_3_HOME_PHONE_COUNTRY_ACCESS_CODE, TRAVELER_3_HOME_PHONE_NUMBER,
            TRAVELER_3_BUSINESS_PHONE_AREA_CODE,TRAVELER_3_BUSINESS_PHONE_COUNTRY_ACCESS_CODE,
            traveler_3_business_phone_number,traveler_3_mobile_phone_area_code,traveler_3_mobile_phone_country_access_code,
            traveler_3_mobile_phone_number,traveler_3_email_address,
            traveler_is_given_name_lower_case_3,traveler_is_given_name_upper_case_3,traveler_is_primary_3,
            traveler_is_surname_lower_case_3,traveler_is_surname_upper_case_3
            ),
            struct(TRAVELER_GIVEN_NAME_4, TRAVELER_MIDDLE_NAME_4, TRAVELER_LAST_NAME_4, TRAVELER_GENDER_NAME_4,
            TRAVELER_4_HOME_PHONE_AREA_CODE,TRAVELER_4_HOME_PHONE_COUNTRY_ACCESS_CODE, TRAVELER_4_HOME_PHONE_NUMBER,
            TRAVELER_4_BUSINESS_PHONE_AREA_CODE,TRAVELER_4_BUSINESS_PHONE_COUNTRY_ACCESS_CODE,
            traveler_4_business_phone_number,traveler_4_mobile_phone_area_code,traveler_4_mobile_phone_country_access_code,
            traveler_4_mobile_phone_number,traveler_4_email_address,
            traveler_is_given_name_lower_case_4,traveler_is_given_name_upper_case_4,traveler_is_primary_4,
            traveler_is_surname_lower_case_4,traveler_is_surname_upper_case_4
            ),
            struct(TRAVELER_GIVEN_NAME_5, TRAVELER_MIDDLE_NAME_5, TRAVELER_LAST_NAME_5, TRAVELER_GENDER_NAME_5,
            TRAVELER_5_HOME_PHONE_AREA_CODE,TRAVELER_5_HOME_PHONE_COUNTRY_ACCESS_CODE, TRAVELER_5_HOME_PHONE_NUMBER,
            TRAVELER_5_BUSINESS_PHONE_AREA_CODE,TRAVELER_5_BUSINESS_PHONE_COUNTRY_ACCESS_CODE,
            traveler_5_business_phone_number,traveler_5_mobile_phone_area_code,traveler_5_mobile_phone_country_access_code,
            traveler_5_mobile_phone_number,traveler_5_email_address,
            traveler_is_given_name_lower_case_5,traveler_is_given_name_upper_case_5,traveler_is_primary_5,
            traveler_is_surname_lower_case_5,traveler_is_surname_upper_case_5
            )
        )
    ) t1 as trv_list
    where
        fraud_transaction_create_date between '${START_DATE}' and '${END_DATE}' and 
        (trv_list.col1 <> '' or trv_list.col2 <> '' or trv_list.col3 <> '' or trv_list.col4 <> '')
    group by fraud_transaction_id
) trvs on trvs.fraud_transaction_id = tr.fraud_transaction_id 


left outer join (
    select 
        fraud_transaction_id,
        exp_collect_struct(
            struct(
                item_list1.col1,
                item_list1.col2,
                item_list1.col3,
                item_list1.col4,
                item_list1.col5
            )
        ) as item
    from etldata.fraud_scoring_xml_model_detail lateral view explode(
        array(
            struct(ITEM_SEGMENT_AIRLINE_1, ITEM_SEGMENT_ARRIVAL_AIRPORT_1, 
            ITEM_SEGMENT_DEPARTURE_AIRPORT_1, ITEM_SEGMENT_IS_DESTINATION_1,
            ITEM_SEGMENT_IS_ORIGIN_1
            ),
            struct(ITEM_SEGMENT_AIRLINE_2, ITEM_SEGMENT_ARRIVAL_AIRPORT_2, 
            ITEM_SEGMENT_DEPARTURE_AIRPORT_2, ITEM_SEGMENT_IS_DESTINATION_2,
            ITEM_SEGMENT_IS_ORIGIN_2
            ),
            struct(ITEM_SEGMENT_AIRLINE_3, ITEM_SEGMENT_ARRIVAL_AIRPORT_3, 
            ITEM_SEGMENT_DEPARTURE_AIRPORT_3, ITEM_SEGMENT_IS_DESTINATION_3,
            ITEM_SEGMENT_IS_ORIGIN_3
            ),
            struct(ITEM_SEGMENT_AIRLINE_4, ITEM_SEGMENT_ARRIVAL_AIRPORT_4, 
            ITEM_SEGMENT_DEPARTURE_AIRPORT_4, ITEM_SEGMENT_IS_DESTINATION_4,
            ITEM_SEGMENT_IS_ORIGIN_4
            ),
            struct(ITEM_SEGMENT_AIRLINE_5, ITEM_SEGMENT_ARRIVAL_AIRPORT_5, 
            ITEM_SEGMENT_DEPARTURE_AIRPORT_5, ITEM_SEGMENT_IS_DESTINATION_5,
            ITEM_SEGMENT_IS_ORIGIN_5
            )
        )
    ) t1 as item_list1
    where
        fraud_transaction_create_date between '${START_DATE}' and '${END_DATE}' and 
        (item_list1.col1 <> '' or item_list1.col2 <> '' or item_list1.col3 <> '' or item_list1.col4 <> '' or item_list1.col5 <> '')
    group by fraud_transaction_id
) items1 on items1.fraud_transaction_id = tr.fraud_transaction_id 




left outer join (
    select 
        fraud_transaction_id,
        max(PAYMENT_ITEM_COUNT) as PAYMENT_ITEM_COUNT,
        exp_collect_struct(
            struct(
                FORM_OF_PAYMENT_TYPE,
			    PAYMENT_ADDRESS_LINE_1,
				PAYMENT_AMOUNT_CURRENCY_CODE,
				PAYMENT_ANY_TRAVELER_SURNAME_NOT_MATCH,
				PAYMENT_AREA_CODE,
				PAYMENT_AVS_RESPONSE_CODE,
				PAYMENT_BIN_NUMBER,
				PAYMENT_CITY,
				PAYMENT_COMPANY_NAME,
				PAYMENT_COUNTRY,
				PAYMENT_COUNTRY_ACCESS_CODE,
				PAYMENT_EXPIRATION_DATE,
				PAYMENT_GIVEN_NAME,
				PAYMENT_IS_ANY_TRAVELER_SURNAME,
				PAYMENT_IS_GIVEN_NAME_UPPER_CASE,
				PAYMENT_IS_PRIMARY_TRAVELER_GIVEN_NAME,
				PAYMENT_IS_PRIMARY_TRAVELER_SURNAME,
				PAYMENT_IS_SURNAME_ANY_TRAV_OR_ACCT,
				PAYMENT_IS_SURNAME_UPPER_CASE,
				PAYMENT_ISSUE_COUNTRY,
				PAYMENT_LAST_NAME,
				PAYMENT_POSTAL_CODE,
				PAYMENT_STATE_PROV,
				PAYMENT_USD_AMOUNT,
				PAYMENT_AUTH_RESPONSE_CODE,
				PAYMENT_AUTH_RESPONSE_GROUP,
				PAYMENT_CSV_CHECK_STATUS,
				PAYMENT_EMAIL_ADDRESS,
				PAYMENT_MIDDLE_NAME,
				PAYMENT_PAYER_ID,
				PAYMENT_PHONE_NUMBER,
				PAYMENT_PHONE_TYPE,
				PAYMENT_ADDRESS_LINE_2,
				PAYMENT_ADDRESS_LINE_3,
				PAYMENT_ADDRESS_LINE_4,
				PAYMENT_LOCAL_AMOUNT,
				concat_ws(' ', PAYMENT_GIVEN_NAME, PAYMENT_MIDDLE_NAME, PAYMENT_LAST_NAME)
            )
        ) as payment
  
    from etldata.fraud_scoring_xml_PAYMENT_detail    
    where 
        fraud_transaction_create_date between '${START_DATE}' and '${END_DATE}' AND PAYMENT_TYPE = 'Credit Card' 
    group by fraud_transaction_id
) pmts on pmts.fraud_transaction_id = tr.fraud_transaction_id


left outer join gfora.frauddata_001 oms1 on oms1.order_id = cast(tr.order_id as bigint)


left outer join gfora.frauddata_002 oms2 on oms2.order_id = cast(tr.order_id as bigint)


left outer join (
    select
        order_id,
        exp_collect_struct( 
            struct (
                order_line_id,
                order_line_type_id,
                booking_record_id,
                booking_item_id,
                tsatravelid,
                frequentflyernumber,
                passportnumber,
                vendorcode,
                inventorytype,
                travelingwithchild,
                refundability
            )
        ) values
    from (
        select * from (
            select row_number(order_id) as num, *
            from (
                select *
                from platdev.seq_frauddata_003
                distribute by order_id
                sort by order_id, order_line_id
            ) t3
        ) t2
        where num < 3
    ) t1
    group by order_id
) oms3 on oms3.order_id = cast(tr.order_id as bigint)


left outer join (
    select 
        fraudtransaction_fraudtransactionid,
        exp_collect_struct(
            struct(
                lacreditcard_hashedcardnumber,
                lacreditcard_cardexpdate
            )
        ) cards
    from lz.fraud_zeus_fraudtransaction_flat
    group by fraudtransaction_fraudtransactionid


) flat2 on flat2.fraudtransaction_fraudtransactionid = cast(tr.FRAUD_TRANSACTION_ID as int)


left outer join (
    select
        fraud_transaction_id,
        count(1) unique_travelers_count,
        collect_set(traveler_full_name) unique_travelers_list
    from (
        select distinct
            fraud_transaction_id,
            concat_ws('_*', 
                traveler.givenName,
                traveler.middleName,
                traveler.surname
            ) traveler_full_name
        from etldata.fraud_scoring_xml_model_detail lateral view explode(traveler_list) tl1 as traveler
        where 
            fraud_transaction_create_date between '${START_DATE}' and '${END_DATE}'
    ) ut1
    group by fraud_transaction_id
) unq_travs on unq_travs.fraud_transaction_id = tr.fraud_transaction_id


left outer join gfora.fraudoutcomeextract02 outcome on outcome.fraudtransactionid =  cast(tr.FRAUD_TRANSACTION_ID as int)


where tr.booking_system_id = 'OMS' and tr.fraud_transaction_create_date between '${START_DATE}' and '${END_DATE}';

