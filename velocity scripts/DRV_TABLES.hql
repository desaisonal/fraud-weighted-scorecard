SET hive.stats.autogather=false;
SET hive.exec.compress.output=true; 
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec; 
SET mapred.output.compression.type=BLOCK;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

USE gfora;

DROP TABLE RAJ_FINAL_TABLE_DRV_1;

CREATE TABLE RAJ_FINAL_TABLE_DRV_1
ROW FORMAT DELIMITED
STORED AS SEQUENCEFILE
AS

SELECT 
a.*,b.*
FROM 
gfora.FINAL_TABLE_1 a
LEFT OUTER JOIN
gfora.MODEL_DETAIL_EMAIL_DRV b
ON
a.fraudtransaction_fraudtransactionid = b.FRAUD_TRANSACTION_ID
;
------------------------------------

DROP TABLE RAJ_FINAL_TABLE_DRV_2;

CREATE TABLE RAJ_FINAL_TABLE_DRV_2
ROW FORMAT DELIMITED
STORED AS SEQUENCEFILE
AS

SELECT 
a.fraudtransaction_fraudtransactionid
,a.fraudtransaction_iscompletedbooking
,a.fraudtransaction_trl
,a.item_amount_local
,a.item_amount_usd_derived
,a.item_currency_code
,a.product_item_count
,a.traveler_phone_number_count
,a.traveler_is_any_given_name_lower_case
,a.traveler_is_any_given_name_upper_case
,a.traveler_is_any_surname_lower_case
,a.traveler_is_any_surname_upper_case
,a.payment_item_count
,a.traveler_email_address_1
,a.traveler_email_address_2
,a.traveler_email_address_3
,a.traveler_email_address_4
,a.traveler_email_address_5
,b.traveler_full_name_derived_1
,b.traveler_given_name_1
,b.traveler_middle_name_1
,b.traveler_last_name_1
,b.traveler_gender_name_1
,b.traveler_is_primary_1
,b.traveler_is_given_name_lower_case_1
,b.traveler_is_given_name_upper_case_1
,b.traveler_is_surname_lower_case_1
,b.traveler_is_surname_upper_case_1
,b.traveler_full_name_derived_2
,b.traveler_given_name_2
,b.traveler_middle_name_2
,b.traveler_last_name_2
,b.traveler_gender_name_2
,b.traveler_is_primary_2
,b.traveler_is_given_name_lower_case_2
,b.traveler_is_given_name_upper_case_2
,b.traveler_is_surname_lower_case_2
,b.traveler_is_surname_upper_case_2
,b.traveler_full_name_derived_3
,b.traveler_given_name_3
,b.traveler_middle_name_3
,b.traveler_last_name_3
,b.traveler_gender_name_3
,b.traveler_is_primary_3
,b.traveler_is_given_name_lower_case_3
,b.traveler_is_given_name_upper_case_3
,b.traveler_is_surname_lower_case_3
,b.traveler_is_surname_upper_case_3
,b.traveler_full_name_derived_4
,b.traveler_given_name_4
,b.traveler_middle_name_4
,b.traveler_last_name_4
,b.traveler_gender_name_4
,b.traveler_is_primary_4
,b.traveler_is_given_name_lower_case_4
,b.traveler_is_given_name_upper_case_4
,b.traveler_is_surname_lower_case_4
,b.traveler_is_surname_upper_case_4
,b.traveler_full_name_derived_5
,b.traveler_given_name_5
,b.traveler_middle_name_5
,b.traveler_last_name_5
,b.traveler_gender_name_5
,b.traveler_is_primary_5
,b.traveler_is_given_name_lower_case_5
,b.traveler_is_given_name_upper_case_5
,b.traveler_is_surname_lower_case_5
,b.traveler_is_surname_upper_case_5
FROM 
gfora.RAJ_FINAL_TABLE_DRV_1 a
LEFT OUTER JOIN
gfora.MODEL_DETAIL_TRAVELERNAME_DRV b
ON
a.fraudtransaction_fraudtransactionid = b.FRAUD_TRANSACTION_ID
;
--------------------------------------------

DROP TABLE RAJ_FINAL_TABLE_DRV_3;

CREATE TABLE RAJ_FINAL_TABLE_DRV_3
ROW FORMAT DELIMITED
STORED AS SEQUENCEFILE
AS

SELECT 
a.*
,b.traveler_business_phone_derived_1
,b.traveler_business_phone_area_code_1
,b.traveler_business_phone_country_access_code_1
,b.traveler_business_phone_number_1
,b.traveler_business_phone_derived_2
,b.traveler_business_phone_area_code_2
,b.traveler_business_phone_country_access_code_2
,b.traveler_business_phone_number_2
,b.traveler_business_phone_derived_3
,b.traveler_business_phone_area_code_3
,b.traveler_business_phone_country_access_code_3
,b.traveler_business_phone_number_3
,b.traveler_business_phone_derived_4
,b.traveler_business_phone_area_code_4
,b.traveler_business_phone_country_access_code_4
,b.traveler_business_phone_number_4
,b.traveler_business_phone_derived_5
,b.traveler_business_phone_area_code_5
,b.traveler_business_phone_country_access_code_5
,b.traveler_business_phone_number_5
FROM 
gfora.RAJ_FINAL_TABLE_DRV_2 a
LEFT OUTER JOIN
gfora.MODEL_DETAIL_BUSINESSPHONE_DRV b
ON
a.fraudtransaction_fraudtransactionid = b.FRAUD_TRANSACTION_ID
;
-------------------------------------

DROP TABLE RAJ_FINAL_TABLE_DRV_4;

CREATE TABLE RAJ_FINAL_TABLE_DRV_4
ROW FORMAT DELIMITED
STORED AS SEQUENCEFILE
AS

SELECT 
a.*
,e.TRAVELER_HOME_PHONE_DERIVED_1
,e.TRAVELER_HOME_PHONE_AREA_CODE_1
,e.TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_1
,e.TRAVELER_HOME_PHONE_NUMBER_1	 
,e.TRAVELER_HOME_PHONE_DERIVED_2
,e.TRAVELER_HOME_PHONE_AREA_CODE_2
,e.TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_2
,e.TRAVELER_HOME_PHONE_NUMBER_2
,e.TRAVELER_HOME_PHONE_DERIVED_3
,e.TRAVELER_HOME_PHONE_AREA_CODE_3
,e.TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_3
,e.TRAVELER_HOME_PHONE_NUMBER_3
,e.TRAVELER_HOME_PHONE_DERIVED_4
,e.TRAVELER_HOME_PHONE_AREA_CODE_4
,e.TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_4
,e.TRAVELER_HOME_PHONE_NUMBER_4
,e.TRAVELER_HOME_PHONE_DERIVED_5
,e.TRAVELER_HOME_PHONE_AREA_CODE_5
,e.TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_5
,e.TRAVELER_HOME_PHONE_NUMBER_5
FROM 
gfora.RAJ_FINAL_TABLE_DRV_3 a
LEFT OUTER JOIN
gfora.MODEL_DETAIL_HOMEPHONE_DRV e
ON
a.fraudtransaction_fraudtransactionid = e.FRAUD_TRANSACTION_ID
;
-----------------------------------

DROP TABLE RAJ_FINAL_TABLE;

CREATE TABLE RAJ_FINAL_TABLE
ROW FORMAT DELIMITED
STORED AS SEQUENCEFILE
AS

SELECT 
 a.fraudtransaction_fraudtransactionid AS FRAUD_TRANSACTION_ID
,a.fraudtransaction_iscompletedbooking AS ISCOMPLETED_BOOKING
,a.fraudtransaction_trl AS TRL
,a.ITEM_AMOUNT_LOCAL
,a.ITEM_AMOUNT_USD_DERIVED
,a.ITEM_CURRENCY_CODE
,a.PRODUCT_ITEM_COUNT
,cast(a.TRAVELER_PHONE_NUMBER_COUNT as int) as TRAVELER_PHONE_NUMBER_COUNT
,a.TRAVELER_IS_ANY_GIVEN_NAME_LOWER_CASE
,a.TRAVELER_IS_ANY_GIVEN_NAME_UPPER_CASE
,a.TRAVELER_IS_ANY_SURNAME_LOWER_CASE
,a.TRAVELER_IS_ANY_SURNAME_UPPER_CASE
,a.PAYMENT_ITEM_COUNT
,a.TRAVELER_EMAIL_ADDRESS_1
,a.TRAVELER_EMAIL_ADDRESS_2
,a.TRAVELER_EMAIL_ADDRESS_3
,a.TRAVELER_EMAIL_ADDRESS_4
,a.TRAVELER_EMAIL_ADDRESS_5 
,a.TRAVELER_FULL_NAME_DERIVED_1
,a.TRAVELER_GIVEN_NAME_1
,a.TRAVELER_MIDDLE_NAME_1
,a.TRAVELER_LAST_NAME_1
,a.TRAVELER_GENDER_NAME_1	 
,a.TRAVELER_IS_PRIMARY_1
,a.TRAVELER_IS_GIVEN_NAME_LOWER_CASE_1
,a.TRAVELER_IS_GIVEN_NAME_UPPER_CASE_1
,a.TRAVELER_IS_SURNAME_LOWER_CASE_1
,a.TRAVELER_IS_SURNAME_UPPER_CASE_1
,a.TRAVELER_FULL_NAME_DERIVED_2
,a.TRAVELER_GIVEN_NAME_2
,a.TRAVELER_MIDDLE_NAME_2
,a.TRAVELER_LAST_NAME_2
,a.TRAVELER_GENDER_NAME_2	 
,a.TRAVELER_IS_PRIMARY_2
,a.TRAVELER_IS_GIVEN_NAME_LOWER_CASE_2
,a.TRAVELER_IS_GIVEN_NAME_UPPER_CASE_2
,a.TRAVELER_IS_SURNAME_LOWER_CASE_2
,a.TRAVELER_IS_SURNAME_UPPER_CASE_2
,a.TRAVELER_FULL_NAME_DERIVED_3
,a.TRAVELER_GIVEN_NAME_3
,a.TRAVELER_MIDDLE_NAME_3
,a.TRAVELER_LAST_NAME_3
,a.TRAVELER_GENDER_NAME_3	 
,a.TRAVELER_IS_PRIMARY_3
,a.TRAVELER_IS_GIVEN_NAME_LOWER_CASE_3
,a.TRAVELER_IS_GIVEN_NAME_UPPER_CASE_3
,a.TRAVELER_IS_SURNAME_LOWER_CASE_3
,a.TRAVELER_IS_SURNAME_UPPER_CASE_3
,a.TRAVELER_FULL_NAME_DERIVED_4
,a.TRAVELER_GIVEN_NAME_4
,a.TRAVELER_MIDDLE_NAME_4
,a.TRAVELER_LAST_NAME_4
,a.TRAVELER_GENDER_NAME_4 
,a.TRAVELER_IS_PRIMARY_4
,a.TRAVELER_IS_GIVEN_NAME_LOWER_CASE_4
,a.TRAVELER_IS_GIVEN_NAME_UPPER_CASE_4
,a.TRAVELER_IS_SURNAME_LOWER_CASE_4
,a.TRAVELER_IS_SURNAME_UPPER_CASE_4
,a.TRAVELER_FULL_NAME_DERIVED_5
,a.TRAVELER_GIVEN_NAME_5
,a.TRAVELER_MIDDLE_NAME_5
,a.TRAVELER_LAST_NAME_5
,a.TRAVELER_GENDER_NAME_5	 
,a.TRAVELER_IS_PRIMARY_5
,a.TRAVELER_IS_GIVEN_NAME_LOWER_CASE_5
,a.TRAVELER_IS_GIVEN_NAME_UPPER_CASE_5
,a.TRAVELER_IS_SURNAME_LOWER_CASE_5
,a.TRAVELER_IS_SURNAME_UPPER_CASE_5
,a.TRAVELER_BUSINESS_PHONE_DERIVED_1
,a.TRAVELER_BUSINESS_PHONE_AREA_CODE_1
,a.TRAVELER_BUSINESS_PHONE_COUNTRY_ACCESS_CODE_1
,a.TRAVELER_BUSINESS_PHONE_NUMBER_1
,a.TRAVELER_BUSINESS_PHONE_DERIVED_2
,a.TRAVELER_BUSINESS_PHONE_AREA_CODE_2
,a.TRAVELER_BUSINESS_PHONE_COUNTRY_ACCESS_CODE_2
,a.TRAVELER_BUSINESS_PHONE_NUMBER_2
,a.TRAVELER_BUSINESS_PHONE_DERIVED_3
,a.TRAVELER_BUSINESS_PHONE_AREA_CODE_3
,a.TRAVELER_BUSINESS_PHONE_COUNTRY_ACCESS_CODE_3
,a.TRAVELER_BUSINESS_PHONE_NUMBER_3
,a.TRAVELER_BUSINESS_PHONE_DERIVED_4
,a.TRAVELER_BUSINESS_PHONE_AREA_CODE_4
,a.TRAVELER_BUSINESS_PHONE_COUNTRY_ACCESS_CODE_4
,a.TRAVELER_BUSINESS_PHONE_NUMBER_4
,a.TRAVELER_BUSINESS_PHONE_DERIVED_5
,a.TRAVELER_BUSINESS_PHONE_AREA_CODE_5
,a.TRAVELER_BUSINESS_PHONE_COUNTRY_ACCESS_CODE_5
,a.TRAVELER_BUSINESS_PHONE_NUMBER_5
,a.TRAVELER_HOME_PHONE_DERIVED_1
,a.TRAVELER_HOME_PHONE_AREA_CODE_1
,a.TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_1
,a.TRAVELER_HOME_PHONE_NUMBER_1	 
,a.TRAVELER_HOME_PHONE_DERIVED_2
,a.TRAVELER_HOME_PHONE_AREA_CODE_2
,a.TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_2
,a.TRAVELER_HOME_PHONE_NUMBER_2
,a.TRAVELER_HOME_PHONE_DERIVED_3
,a.TRAVELER_HOME_PHONE_AREA_CODE_3
,a.TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_3
,a.TRAVELER_HOME_PHONE_NUMBER_3
,a.TRAVELER_HOME_PHONE_DERIVED_4
,a.TRAVELER_HOME_PHONE_AREA_CODE_4
,a.TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_4
,a.TRAVELER_HOME_PHONE_NUMBER_4
,a.TRAVELER_HOME_PHONE_DERIVED_5
,a.TRAVELER_HOME_PHONE_AREA_CODE_5
,a.TRAVELER_HOME_PHONE_COUNTRY_ACCESS_CODE_5
,a.TRAVELER_HOME_PHONE_NUMBER_5
,f.TRAVELER_MOBILE_PHONE_DERIVED_1
,f.TRAVELER_MOBILE_PHONE_AREA_CODE_1
,f.TRAVELER_MOBILE_PHONE_COUNTRY_ACCESS_CODE_1
,f.TRAVELER_MOBILE_PHONE_NUMBER_1	 
,f.TRAVELER_MOBILE_PHONE_DERIVED_2
,f.TRAVELER_MOBILE_PHONE_AREA_CODE_2
,f.TRAVELER_MOBILE_PHONE_COUNTRY_ACCESS_CODE_2
,f.TRAVELER_MOBILE_PHONE_NUMBER_2
,f.TRAVELER_MOBILE_PHONE_DERIVED_3
,f.TRAVELER_MOBILE_PHONE_AREA_CODE_3
,f.TRAVELER_MOBILE_PHONE_COUNTRY_ACCESS_CODE_3
,f.TRAVELER_MOBILE_PHONE_NUMBER_3
,f.TRAVELER_MOBILE_PHONE_DERIVED_4
,f.TRAVELER_MOBILE_PHONE_AREA_CODE_4
,f.TRAVELER_MOBILE_PHONE_COUNTRY_ACCESS_CODE_4
,f.TRAVELER_MOBILE_PHONE_NUMBER_4
,f.TRAVELER_MOBILE_PHONE_DERIVED_5
,f.TRAVELER_MOBILE_PHONE_AREA_CODE_5
,f.TRAVELER_MOBILE_PHONE_COUNTRY_ACCESS_CODE_5
,f.TRAVELER_MOBILE_PHONE_NUMBER_5
FROM 
gfora.RAJ_FINAL_TABLE_DRV_4 a
LEFT OUTER JOIN
gfora.MODEL_DETAIL_MOBILEPHONE_DRV f
ON
a.fraudtransaction_fraudtransactionid = f.FRAUD_TRANSACTION_ID
;

