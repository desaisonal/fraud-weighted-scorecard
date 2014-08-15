add jar hive-udfs-1.1.1-cdh4-SNAPSHOT.jar;

create temporary function exp_sliding_count as 'com.expedia.edw.hive.udf.window.SlidingCount';
create temporary function exp_sliding_sum as 'com.expedia.edw.hive.udf.window.SlidingSum';
create temporary function exp_sliding_bidist_count as 'com.expedia.edw.hive.udf.window.SlidingBiDistinctCount';

SET hive.variable.substitute.depth=200;
SET hive.exec.parallel=true;
SET hive.stats.autogather=false;

SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

use gfora;

create table ${TARGET_TABLE} 
ROW FORMAT DELIMITED STORED AS SEQUENCEFILE AS
select 
  src.fraud_transaction_id


  ,account_home_phone_country_access_code_30days_COUNT
  ,account_home_phone_country_access_code_payment_usd_amount_1_30days_SUM


  ,ip_continent_30days_COUNT
  ,ip_continent_payment_usd_amount_1_30days_SUM


  ,accountnum_30days_COUNT
  ,accountnum_payment_usd_amount_1_30days_SUM


  ,ip_address_cidr_30days_COUNT
  ,ip_address_cidr_payment_usd_amount_1_30days_SUM


  ,ip_top_level_domain_30days_COUNT
  ,ip_top_level_domain_payment_usd_amount_1_30days_SUM


  ,account_company_name_30days_COUNT
  ,account_company_name_payment_usd_amount_1_30days_SUM


  ,account_person_name_30days_COUNT
  ,account_person_name_payment_usd_amount_1_30days_SUM


  ,frequentflyernumber_1_30days_COUNT
  ,frequentflyernumber_1_payment_usd_amount_1_30days_SUM
  ,frequentflyernumber_1_ip_address_30days_BI_COUNT
  ,frequentflyernumber_1_account_email_30days_BI_COUNT
  ,frequentflyernumber_1_account_id_30days_BI_COUNT
  ,frequentflyernumber_1_account_password_30days_BI_COUNT
  ,frequentflyernumber_1_traveler_email_address_1_30days_BI_COUNT
  ,frequentflyernumber_1_payment_email_address_1_30days_BI_COUNT
  ,frequentflyernumber_1_payment_phone_number_1_30days_BI_COUNT
  ,frequentflyernumber_1_tsatravelid_1_30days_BI_COUNT
  ,frequentflyernumber_1_passportnumber_1_30days_BI_COUNT
  ,frequentflyernumber_1_payment_hashed_credit_card_number_1_30days_BI_COUNT
  ,frequentflyernumber_1_item_destination_1_30days_BI_COUNT


  ,ip_city_30days_COUNT
  ,ip_city_payment_usd_amount_1_30days_SUM


  ,tsatravelid_1_30days_COUNT
  ,tsatravelid_1_payment_usd_amount_1_30days_SUM
  ,tsatravelid_1_ip_address_30days_BI_COUNT
  ,tsatravelid_1_account_email_30days_BI_COUNT
  ,tsatravelid_1_account_id_30days_BI_COUNT
  ,tsatravelid_1_account_password_30days_BI_COUNT
  ,tsatravelid_1_traveler_email_address_1_30days_BI_COUNT
  ,tsatravelid_1_payment_email_address_1_30days_BI_COUNT
  ,tsatravelid_1_payment_phone_number_1_30days_BI_COUNT
  ,tsatravelid_1_frequentflyernumber_1_30days_BI_COUNT
  ,tsatravelid_1_passportnumber_1_30days_BI_COUNT
  ,tsatravelid_1_payment_hashed_credit_card_number_1_30days_BI_COUNT
  ,tsatravelid_1_item_destination_1_30days_BI_COUNT


  ,ip_postal_code_30days_COUNT
  ,ip_postal_code_payment_usd_amount_1_30days_SUM


  ,item_postal_code_1_30days_COUNT
  ,item_postal_code_1_payment_usd_amount_1_30days_SUM


  ,account_password_30days_COUNT
  ,account_password_payment_usd_amount_1_30days_SUM
  ,account_password_ip_address_30days_BI_COUNT
  ,account_password_account_email_30days_BI_COUNT
  ,account_password_account_id_30days_BI_COUNT
  ,account_password_traveler_email_address_1_30days_BI_COUNT
  ,account_password_payment_email_address_1_30days_BI_COUNT
  ,account_password_payment_phone_number_1_30days_BI_COUNT
  ,account_password_tsatravelid_1_30days_BI_COUNT
  ,account_password_frequentflyernumber_1_30days_BI_COUNT
  ,account_password_passportnumber_1_30days_BI_COUNT
  ,account_password_payment_hashed_credit_card_number_1_30days_BI_COUNT
  ,account_password_item_destination_1_30days_BI_COUNT


  ,payment_last_name_1_30days_COUNT
  ,payment_last_name_1_payment_usd_amount_1_30days_SUM


  ,payment_email_address_1_30days_COUNT
  ,payment_email_address_1_payment_usd_amount_1_30days_SUM
  ,payment_email_address_1_ip_address_30days_BI_COUNT
  ,payment_email_address_1_account_email_30days_BI_COUNT
  ,payment_email_address_1_account_id_30days_BI_COUNT
  ,payment_email_address_1_account_password_30days_BI_COUNT
  ,payment_email_address_1_traveler_email_address_1_30days_BI_COUNT
  ,payment_email_address_1_payment_phone_number_1_30days_BI_COUNT
  ,payment_email_address_1_tsatravelid_1_30days_BI_COUNT
  ,payment_email_address_1_frequentflyernumber_1_30days_BI_COUNT
  ,payment_email_address_1_passportnumber_1_30days_BI_COUNT
  ,payment_email_address_1_payment_hashed_credit_card_number_1_30days_BI_COUNT
  ,payment_email_address_1_item_destination_1_30days_BI_COUNT


  ,ip_routing_method_30days_COUNT
  ,ip_routing_method_payment_usd_amount_1_30days_SUM


  ,traveler_full_name_1_30days_COUNT
  ,traveler_full_name_1_payment_usd_amount_1_30days_SUM


  ,item_airport_pick_up_location_1_30days_COUNT
  ,item_airport_pick_up_location_1_payment_usd_amount_1_30days_SUM


  ,account_email_30days_COUNT
  ,account_email_payment_usd_amount_1_30days_SUM
  ,account_email_ip_address_30days_BI_COUNT
  ,account_email_account_id_30days_BI_COUNT
  ,account_email_account_password_30days_BI_COUNT
  ,account_email_traveler_email_address_1_30days_BI_COUNT
  ,account_email_payment_email_address_1_30days_BI_COUNT
  ,account_email_payment_phone_number_1_30days_BI_COUNT
  ,account_email_tsatravelid_1_30days_BI_COUNT
  ,account_email_frequentflyernumber_1_30days_BI_COUNT
  ,account_email_passportnumber_1_30days_BI_COUNT
  ,account_email_payment_hashed_credit_card_number_1_30days_BI_COUNT
  ,account_email_item_destination_1_30days_BI_COUNT


  ,payment_issue_country_1_30days_COUNT
  ,payment_issue_country_1_payment_usd_amount_1_30days_SUM


  ,passportnumber_1_30days_COUNT
  ,passportnumber_1_payment_usd_amount_1_30days_SUM
  ,passportnumber_1_ip_address_30days_BI_COUNT
  ,passportnumber_1_account_email_30days_BI_COUNT
  ,passportnumber_1_account_id_30days_BI_COUNT
  ,passportnumber_1_account_password_30days_BI_COUNT
  ,passportnumber_1_traveler_email_address_1_30days_BI_COUNT
  ,passportnumber_1_payment_email_address_1_30days_BI_COUNT
  ,passportnumber_1_payment_phone_number_1_30days_BI_COUNT
  ,passportnumber_1_tsatravelid_1_30days_BI_COUNT
  ,passportnumber_1_frequentflyernumber_1_30days_BI_COUNT
  ,passportnumber_1_payment_hashed_credit_card_number_1_30days_BI_COUNT
  ,passportnumber_1_item_destination_1_30days_BI_COUNT


  ,item_city_1_30days_COUNT
  ,item_city_1_payment_usd_amount_1_30days_SUM


  ,item_vendor_name_1_30days_COUNT
  ,item_vendor_name_1_payment_usd_amount_1_30days_SUM


  ,machine_id_30days_COUNT
  ,machine_id_payment_usd_amount_1_30days_SUM


  ,ip_carrier_30days_COUNT
  ,ip_carrier_payment_usd_amount_1_30days_SUM


  ,account_id_30days_COUNT
  ,account_id_payment_usd_amount_1_30days_SUM
  ,account_id_ip_address_30days_BI_COUNT
  ,account_id_account_email_30days_BI_COUNT
  ,account_id_account_password_30days_BI_COUNT
  ,account_id_traveler_email_address_1_30days_BI_COUNT
  ,account_id_payment_email_address_1_30days_BI_COUNT
  ,account_id_payment_phone_number_1_30days_BI_COUNT
  ,account_id_tsatravelid_1_30days_BI_COUNT
  ,account_id_frequentflyernumber_1_30days_BI_COUNT
  ,account_id_passportnumber_1_30days_BI_COUNT
  ,account_id_payment_hashed_credit_card_number_1_30days_BI_COUNT
  ,account_id_item_destination_1_30days_BI_COUNT


  ,item_destination_1_30days_COUNT
  ,item_destination_1_payment_usd_amount_1_30days_SUM


  ,account_home_phone_area_code_30days_COUNT
  ,account_home_phone_area_code_payment_usd_amount_1_30days_SUM


  ,item_airport_drop_off_location_1_30days_COUNT
  ,item_airport_drop_off_location_1_payment_usd_amount_1_30days_SUM


  ,ip_country_30days_COUNT
  ,ip_country_payment_usd_amount_1_30days_SUM


  ,transaction_guid_30days_COUNT
  ,transaction_guid_payment_usd_amount_1_30days_SUM


  ,ip_address_asn_30days_COUNT
  ,ip_address_asn_payment_usd_amount_1_30days_SUM


  ,ip_time_zone_30days_COUNT
  ,ip_time_zone_payment_usd_amount_1_30days_SUM


  ,ip_area_code_30days_COUNT
  ,ip_area_code_payment_usd_amount_1_30days_SUM


  ,loyalty_member_id_30days_COUNT
  ,loyalty_member_id_payment_usd_amount_1_30days_SUM


  ,payment_hashed_credit_card_number_1_30days_COUNT
  ,payment_hashed_credit_card_number_1_payment_usd_amount_1_30days_SUM
  ,payment_hashed_credit_card_number_1_ip_address_30days_BI_COUNT
  ,payment_hashed_credit_card_number_1_account_email_30days_BI_COUNT
  ,payment_hashed_credit_card_number_1_account_id_30days_BI_COUNT
  ,payment_hashed_credit_card_number_1_account_password_30days_BI_COUNT
  ,payment_hashed_credit_card_number_1_traveler_email_address_1_30days_BI_COUNT
  ,payment_hashed_credit_card_number_1_payment_email_address_1_30days_BI_COUNT
  ,payment_hashed_credit_card_number_1_payment_phone_number_1_30days_BI_COUNT
  ,payment_hashed_credit_card_number_1_tsatravelid_1_30days_BI_COUNT
  ,payment_hashed_credit_card_number_1_frequentflyernumber_1_30days_BI_COUNT
  ,payment_hashed_credit_card_number_1_passportnumber_1_30days_BI_COUNT
  ,payment_hashed_credit_card_number_1_item_destination_1_30days_BI_COUNT


  ,payment_phone_number_1_30days_COUNT
  ,payment_phone_number_1_payment_usd_amount_1_30days_SUM
  ,payment_phone_number_1_ip_address_30days_BI_COUNT
  ,payment_phone_number_1_account_email_30days_BI_COUNT
  ,payment_phone_number_1_account_id_30days_BI_COUNT
  ,payment_phone_number_1_account_password_30days_BI_COUNT
  ,payment_phone_number_1_traveler_email_address_1_30days_BI_COUNT
  ,payment_phone_number_1_payment_email_address_1_30days_BI_COUNT
  ,payment_phone_number_1_tsatravelid_1_30days_BI_COUNT
  ,payment_phone_number_1_frequentflyernumber_1_30days_BI_COUNT
  ,payment_phone_number_1_passportnumber_1_30days_BI_COUNT
  ,payment_phone_number_1_payment_hashed_credit_card_number_1_30days_BI_COUNT
  ,payment_phone_number_1_item_destination_1_30days_BI_COUNT


  ,traveler_email_address_1_30days_COUNT
  ,traveler_email_address_1_payment_usd_amount_1_30days_SUM
  ,traveler_email_address_1_ip_address_30days_BI_COUNT
  ,traveler_email_address_1_account_email_30days_BI_COUNT
  ,traveler_email_address_1_account_id_30days_BI_COUNT
  ,traveler_email_address_1_account_password_30days_BI_COUNT
  ,traveler_email_address_1_payment_email_address_1_30days_BI_COUNT
  ,traveler_email_address_1_payment_phone_number_1_30days_BI_COUNT
  ,traveler_email_address_1_tsatravelid_1_30days_BI_COUNT
  ,traveler_email_address_1_frequentflyernumber_1_30days_BI_COUNT
  ,traveler_email_address_1_passportnumber_1_30days_BI_COUNT
  ,traveler_email_address_1_payment_hashed_credit_card_number_1_30days_BI_COUNT
  ,traveler_email_address_1_item_destination_1_30days_BI_COUNT


  ,ip_address_30days_COUNT
  ,ip_address_payment_usd_amount_1_30days_SUM
  ,ip_address_account_email_30days_BI_COUNT
  ,ip_address_account_id_30days_BI_COUNT
  ,ip_address_account_password_30days_BI_COUNT
  ,ip_address_traveler_email_address_1_30days_BI_COUNT
  ,ip_address_payment_email_address_1_30days_BI_COUNT
  ,ip_address_payment_phone_number_1_30days_BI_COUNT
  ,ip_address_tsatravelid_1_30days_BI_COUNT
  ,ip_address_frequentflyernumber_1_30days_BI_COUNT
  ,ip_address_passportnumber_1_30days_BI_COUNT
  ,ip_address_payment_hashed_credit_card_number_1_30days_BI_COUNT
  ,ip_address_item_destination_1_30days_BI_COUNT


  ,vendorcode_1_30days_COUNT
  ,vendorcode_1_payment_usd_amount_1_30days_SUM


  ,ip_second_level_domain_30days_COUNT
  ,ip_second_level_domain_payment_usd_amount_1_30days_SUM


  ,payment_bin_number_1_30days_COUNT
  ,payment_bin_number_1_payment_usd_amount_1_30days_SUM


  ,account_home_phone_number_30days_COUNT
  ,account_home_phone_number_payment_usd_amount_1_30days_SUM


  ,item_airports_1_30days_COUNT
  ,item_airports_1_payment_usd_amount_1_30days_SUM


  ,accountphonenum_30days_COUNT
  ,accountphonenum_payment_usd_amount_1_30days_SUM


  ,ip_reg_org_30days_COUNT
  ,ip_reg_org_payment_usd_amount_1_30days_SUM


  ,ip_state_30days_COUNT
  ,ip_state_payment_usd_amount_1_30days_SUM


  ,item_hotel_id_1_30days_COUNT
  ,item_hotel_id_1_payment_usd_amount_1_30days_SUM




from (
  select cast(fraud_transaction_id as int) as fraud_transaction_id
  from gfora.${SRC_TABLE}
) src


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', account_home_phone_country_access_code) as account_home_phone_country_access_code_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', account_home_phone_country_access_code)/10000.0 as double) as account_home_phone_country_access_code_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where account_home_phone_country_access_code is not null and account_home_phone_country_access_code != ''
    distribute by account_home_phone_country_access_code
    sort by account_home_phone_country_access_code, fraud_transaction_create_datetm
  ) subq
) j_account_home_phone_country_access_code on j_account_home_phone_country_access_code.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_continent) as ip_continent_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_continent)/10000.0 as double) as ip_continent_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_continent is not null and ip_continent != ''
    distribute by ip_continent
    sort by ip_continent, fraud_transaction_create_datetm
  ) subq
) j_ip_continent on j_ip_continent.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', accountnum) as accountnum_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', accountnum)/10000.0 as double) as accountnum_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where accountnum is not null and accountnum != ''
    distribute by accountnum
    sort by accountnum, fraud_transaction_create_datetm
  ) subq
) j_accountnum on j_accountnum.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_address_cidr) as ip_address_cidr_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_address_cidr)/10000.0 as double) as ip_address_cidr_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_address_cidr is not null and ip_address_cidr != ''
    distribute by ip_address_cidr
    sort by ip_address_cidr, fraud_transaction_create_datetm
  ) subq
) j_ip_address_cidr on j_ip_address_cidr.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_top_level_domain) as ip_top_level_domain_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_top_level_domain)/10000.0 as double) as ip_top_level_domain_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_top_level_domain is not null and ip_top_level_domain != ''
    distribute by ip_top_level_domain
    sort by ip_top_level_domain, fraud_transaction_create_datetm
  ) subq
) j_ip_top_level_domain on j_ip_top_level_domain.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', account_company_name) as account_company_name_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', account_company_name)/10000.0 as double) as account_company_name_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where account_company_name is not null and account_company_name != ''
    distribute by account_company_name
    sort by account_company_name, fraud_transaction_create_datetm
  ) subq
) j_account_company_name on j_account_company_name.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', account_person_name) as account_person_name_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', account_person_name)/10000.0 as double) as account_person_name_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where account_person_name is not null and account_person_name != ''
    distribute by account_person_name
    sort by account_person_name, fraud_transaction_create_datetm
  ) subq
) j_account_person_name on j_account_person_name.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', frequentflyernumber_1) as frequentflyernumber_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', frequentflyernumber_1)/10000.0 as double) as frequentflyernumber_1_payment_usd_amount_1_30days_SUM
    ,exp_sliding_bidist_count(ip_address, fraud_transaction_create_datetm, 30, 'days', frequentflyernumber_1) as frequentflyernumber_1_ip_address_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_email, fraud_transaction_create_datetm, 30, 'days', frequentflyernumber_1) as frequentflyernumber_1_account_email_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_id, fraud_transaction_create_datetm, 30, 'days', frequentflyernumber_1) as frequentflyernumber_1_account_id_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_password, fraud_transaction_create_datetm, 30, 'days', frequentflyernumber_1) as frequentflyernumber_1_account_password_30days_BI_COUNT
    ,exp_sliding_bidist_count(traveler_email_address_1, fraud_transaction_create_datetm, 30, 'days', frequentflyernumber_1) as frequentflyernumber_1_traveler_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_email_address_1, fraud_transaction_create_datetm, 30, 'days', frequentflyernumber_1) as frequentflyernumber_1_payment_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_phone_number_1, fraud_transaction_create_datetm, 30, 'days', frequentflyernumber_1) as frequentflyernumber_1_payment_phone_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(tsatravelid_1, fraud_transaction_create_datetm, 30, 'days', frequentflyernumber_1) as frequentflyernumber_1_tsatravelid_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(passportnumber_1, fraud_transaction_create_datetm, 30, 'days', frequentflyernumber_1) as frequentflyernumber_1_passportnumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_hashed_credit_card_number_1, fraud_transaction_create_datetm, 30, 'days', frequentflyernumber_1) as frequentflyernumber_1_payment_hashed_credit_card_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(item_destination_1, fraud_transaction_create_datetm, 30, 'days', frequentflyernumber_1) as frequentflyernumber_1_item_destination_1_30days_BI_COUNT
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where frequentflyernumber_1 is not null and frequentflyernumber_1 != '' and frequentflyernumber_1 != 'NotAvailable' and frequentflyernumber_1 != '*Note#2*'
    distribute by frequentflyernumber_1
    sort by frequentflyernumber_1, fraud_transaction_create_datetm
  ) subq
) j_frequentflyernumber_1 on j_frequentflyernumber_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_city) as ip_city_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_city)/10000.0 as double) as ip_city_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_city is not null and ip_city != ''
    distribute by ip_city
    sort by ip_city, fraud_transaction_create_datetm
  ) subq
) j_ip_city on j_ip_city.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', tsatravelid_1) as tsatravelid_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', tsatravelid_1)/10000.0 as double) as tsatravelid_1_payment_usd_amount_1_30days_SUM
    ,exp_sliding_bidist_count(ip_address, fraud_transaction_create_datetm, 30, 'days', tsatravelid_1) as tsatravelid_1_ip_address_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_email, fraud_transaction_create_datetm, 30, 'days', tsatravelid_1) as tsatravelid_1_account_email_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_id, fraud_transaction_create_datetm, 30, 'days', tsatravelid_1) as tsatravelid_1_account_id_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_password, fraud_transaction_create_datetm, 30, 'days', tsatravelid_1) as tsatravelid_1_account_password_30days_BI_COUNT
    ,exp_sliding_bidist_count(traveler_email_address_1, fraud_transaction_create_datetm, 30, 'days', tsatravelid_1) as tsatravelid_1_traveler_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_email_address_1, fraud_transaction_create_datetm, 30, 'days', tsatravelid_1) as tsatravelid_1_payment_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_phone_number_1, fraud_transaction_create_datetm, 30, 'days', tsatravelid_1) as tsatravelid_1_payment_phone_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(frequentflyernumber_1, fraud_transaction_create_datetm, 30, 'days', tsatravelid_1) as tsatravelid_1_frequentflyernumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(passportnumber_1, fraud_transaction_create_datetm, 30, 'days', tsatravelid_1) as tsatravelid_1_passportnumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_hashed_credit_card_number_1, fraud_transaction_create_datetm, 30, 'days', tsatravelid_1) as tsatravelid_1_payment_hashed_credit_card_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(item_destination_1, fraud_transaction_create_datetm, 30, 'days', tsatravelid_1) as tsatravelid_1_item_destination_1_30days_BI_COUNT
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where tsatravelid_1 is not null and tsatravelid_1 != '' and tsatravelid_1 != 'NotAvailable' and tsatravelid_1 != '*Note#1*'
    distribute by tsatravelid_1
    sort by tsatravelid_1, fraud_transaction_create_datetm
  ) subq
) j_tsatravelid_1 on j_tsatravelid_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_postal_code) as ip_postal_code_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_postal_code)/10000.0 as double) as ip_postal_code_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_postal_code is not null and ip_postal_code != ''
    distribute by ip_postal_code
    sort by ip_postal_code, fraud_transaction_create_datetm
  ) subq
) j_ip_postal_code on j_ip_postal_code.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', item_postal_code_1) as item_postal_code_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', item_postal_code_1)/10000.0 as double) as item_postal_code_1_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where item_postal_code_1 is not null and item_postal_code_1 != ''
    distribute by item_postal_code_1
    sort by item_postal_code_1, fraud_transaction_create_datetm
  ) subq
) j_item_postal_code_1 on j_item_postal_code_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', account_password) as account_password_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', account_password)/10000.0 as double) as account_password_payment_usd_amount_1_30days_SUM
    ,exp_sliding_bidist_count(ip_address, fraud_transaction_create_datetm, 30, 'days', account_password) as account_password_ip_address_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_email, fraud_transaction_create_datetm, 30, 'days', account_password) as account_password_account_email_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_id, fraud_transaction_create_datetm, 30, 'days', account_password) as account_password_account_id_30days_BI_COUNT
    ,exp_sliding_bidist_count(traveler_email_address_1, fraud_transaction_create_datetm, 30, 'days', account_password) as account_password_traveler_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_email_address_1, fraud_transaction_create_datetm, 30, 'days', account_password) as account_password_payment_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_phone_number_1, fraud_transaction_create_datetm, 30, 'days', account_password) as account_password_payment_phone_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(tsatravelid_1, fraud_transaction_create_datetm, 30, 'days', account_password) as account_password_tsatravelid_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(frequentflyernumber_1, fraud_transaction_create_datetm, 30, 'days', account_password) as account_password_frequentflyernumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(passportnumber_1, fraud_transaction_create_datetm, 30, 'days', account_password) as account_password_passportnumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_hashed_credit_card_number_1, fraud_transaction_create_datetm, 30, 'days', account_password) as account_password_payment_hashed_credit_card_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(item_destination_1, fraud_transaction_create_datetm, 30, 'days', account_password) as account_password_item_destination_1_30days_BI_COUNT
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where account_password is not null and account_password != ''
    distribute by account_password
    sort by account_password, fraud_transaction_create_datetm
  ) subq
) j_account_password on j_account_password.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', payment_last_name_1) as payment_last_name_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', payment_last_name_1)/10000.0 as double) as payment_last_name_1_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where payment_last_name_1 is not null and payment_last_name_1 != ''
    distribute by payment_last_name_1
    sort by payment_last_name_1, fraud_transaction_create_datetm
  ) subq
) j_payment_last_name_1 on j_payment_last_name_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', payment_email_address_1) as payment_email_address_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', payment_email_address_1)/10000.0 as double) as payment_email_address_1_payment_usd_amount_1_30days_SUM
    ,exp_sliding_bidist_count(ip_address, fraud_transaction_create_datetm, 30, 'days', payment_email_address_1) as payment_email_address_1_ip_address_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_email, fraud_transaction_create_datetm, 30, 'days', payment_email_address_1) as payment_email_address_1_account_email_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_id, fraud_transaction_create_datetm, 30, 'days', payment_email_address_1) as payment_email_address_1_account_id_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_password, fraud_transaction_create_datetm, 30, 'days', payment_email_address_1) as payment_email_address_1_account_password_30days_BI_COUNT
    ,exp_sliding_bidist_count(traveler_email_address_1, fraud_transaction_create_datetm, 30, 'days', payment_email_address_1) as payment_email_address_1_traveler_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_phone_number_1, fraud_transaction_create_datetm, 30, 'days', payment_email_address_1) as payment_email_address_1_payment_phone_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(tsatravelid_1, fraud_transaction_create_datetm, 30, 'days', payment_email_address_1) as payment_email_address_1_tsatravelid_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(frequentflyernumber_1, fraud_transaction_create_datetm, 30, 'days', payment_email_address_1) as payment_email_address_1_frequentflyernumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(passportnumber_1, fraud_transaction_create_datetm, 30, 'days', payment_email_address_1) as payment_email_address_1_passportnumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_hashed_credit_card_number_1, fraud_transaction_create_datetm, 30, 'days', payment_email_address_1) as payment_email_address_1_payment_hashed_credit_card_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(item_destination_1, fraud_transaction_create_datetm, 30, 'days', payment_email_address_1) as payment_email_address_1_item_destination_1_30days_BI_COUNT
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where payment_email_address_1 is not null and payment_email_address_1 != ''
    distribute by payment_email_address_1
    sort by payment_email_address_1, fraud_transaction_create_datetm
  ) subq
) j_payment_email_address_1 on j_payment_email_address_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_routing_method) as ip_routing_method_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_routing_method)/10000.0 as double) as ip_routing_method_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_routing_method is not null and ip_routing_method != ''
    distribute by ip_routing_method
    sort by ip_routing_method, fraud_transaction_create_datetm
  ) subq
) j_ip_routing_method on j_ip_routing_method.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', traveler_full_name_1) as traveler_full_name_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', traveler_full_name_1)/10000.0 as double) as traveler_full_name_1_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where traveler_full_name_1 is not null and traveler_full_name_1 != ''
    distribute by traveler_full_name_1
    sort by traveler_full_name_1, fraud_transaction_create_datetm
  ) subq
) j_traveler_full_name_1 on j_traveler_full_name_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', item_airport_pick_up_location_1) as item_airport_pick_up_location_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', item_airport_pick_up_location_1)/10000.0 as double) as item_airport_pick_up_location_1_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where item_airport_pick_up_location_1 is not null and item_airport_pick_up_location_1 != ''
    distribute by item_airport_pick_up_location_1
    sort by item_airport_pick_up_location_1, fraud_transaction_create_datetm
  ) subq
) j_item_airport_pick_up_location_1 on j_item_airport_pick_up_location_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', account_email) as account_email_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', account_email)/10000.0 as double) as account_email_payment_usd_amount_1_30days_SUM
    ,exp_sliding_bidist_count(ip_address, fraud_transaction_create_datetm, 30, 'days', account_email) as account_email_ip_address_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_id, fraud_transaction_create_datetm, 30, 'days', account_email) as account_email_account_id_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_password, fraud_transaction_create_datetm, 30, 'days', account_email) as account_email_account_password_30days_BI_COUNT
    ,exp_sliding_bidist_count(traveler_email_address_1, fraud_transaction_create_datetm, 30, 'days', account_email) as account_email_traveler_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_email_address_1, fraud_transaction_create_datetm, 30, 'days', account_email) as account_email_payment_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_phone_number_1, fraud_transaction_create_datetm, 30, 'days', account_email) as account_email_payment_phone_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(tsatravelid_1, fraud_transaction_create_datetm, 30, 'days', account_email) as account_email_tsatravelid_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(frequentflyernumber_1, fraud_transaction_create_datetm, 30, 'days', account_email) as account_email_frequentflyernumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(passportnumber_1, fraud_transaction_create_datetm, 30, 'days', account_email) as account_email_passportnumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_hashed_credit_card_number_1, fraud_transaction_create_datetm, 30, 'days', account_email) as account_email_payment_hashed_credit_card_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(item_destination_1, fraud_transaction_create_datetm, 30, 'days', account_email) as account_email_item_destination_1_30days_BI_COUNT
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where account_email is not null and account_email != ''
    distribute by account_email
    sort by account_email, fraud_transaction_create_datetm
  ) subq
) j_account_email on j_account_email.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', payment_issue_country_1) as payment_issue_country_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', payment_issue_country_1)/10000.0 as double) as payment_issue_country_1_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where payment_issue_country_1 is not null and payment_issue_country_1 != ''
    distribute by payment_issue_country_1
    sort by payment_issue_country_1, fraud_transaction_create_datetm
  ) subq
) j_payment_issue_country_1 on j_payment_issue_country_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', passportnumber_1) as passportnumber_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', passportnumber_1)/10000.0 as double) as passportnumber_1_payment_usd_amount_1_30days_SUM
    ,exp_sliding_bidist_count(ip_address, fraud_transaction_create_datetm, 30, 'days', passportnumber_1) as passportnumber_1_ip_address_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_email, fraud_transaction_create_datetm, 30, 'days', passportnumber_1) as passportnumber_1_account_email_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_id, fraud_transaction_create_datetm, 30, 'days', passportnumber_1) as passportnumber_1_account_id_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_password, fraud_transaction_create_datetm, 30, 'days', passportnumber_1) as passportnumber_1_account_password_30days_BI_COUNT
    ,exp_sliding_bidist_count(traveler_email_address_1, fraud_transaction_create_datetm, 30, 'days', passportnumber_1) as passportnumber_1_traveler_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_email_address_1, fraud_transaction_create_datetm, 30, 'days', passportnumber_1) as passportnumber_1_payment_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_phone_number_1, fraud_transaction_create_datetm, 30, 'days', passportnumber_1) as passportnumber_1_payment_phone_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(tsatravelid_1, fraud_transaction_create_datetm, 30, 'days', passportnumber_1) as passportnumber_1_tsatravelid_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(frequentflyernumber_1, fraud_transaction_create_datetm, 30, 'days', passportnumber_1) as passportnumber_1_frequentflyernumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_hashed_credit_card_number_1, fraud_transaction_create_datetm, 30, 'days', passportnumber_1) as passportnumber_1_payment_hashed_credit_card_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(item_destination_1, fraud_transaction_create_datetm, 30, 'days', passportnumber_1) as passportnumber_1_item_destination_1_30days_BI_COUNT
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where passportnumber_1 is not null and passportnumber_1 != '' and passportnumber_1 != 'NotAvailable' and passportnumber_1 != '*Note#3*'
    distribute by passportnumber_1
    sort by passportnumber_1, fraud_transaction_create_datetm
  ) subq
) j_passportnumber_1 on j_passportnumber_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', item_city_1) as item_city_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', item_city_1)/10000.0 as double) as item_city_1_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where item_city_1 is not null and item_city_1 != ''
    distribute by item_city_1
    sort by item_city_1, fraud_transaction_create_datetm
  ) subq
) j_item_city_1 on j_item_city_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', item_vendor_name_1) as item_vendor_name_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', item_vendor_name_1)/10000.0 as double) as item_vendor_name_1_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where item_vendor_name_1 is not null and item_vendor_name_1 != ''
    distribute by item_vendor_name_1
    sort by item_vendor_name_1, fraud_transaction_create_datetm
  ) subq
) j_item_vendor_name_1 on j_item_vendor_name_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', machine_id) as machine_id_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', machine_id)/10000.0 as double) as machine_id_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where machine_id is not null and machine_id != ''
    distribute by machine_id
    sort by machine_id, fraud_transaction_create_datetm
  ) subq
) j_machine_id on j_machine_id.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_carrier) as ip_carrier_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_carrier)/10000.0 as double) as ip_carrier_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_carrier is not null and ip_carrier != ''
    distribute by ip_carrier
    sort by ip_carrier, fraud_transaction_create_datetm
  ) subq
) j_ip_carrier on j_ip_carrier.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', account_id) as account_id_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', account_id)/10000.0 as double) as account_id_payment_usd_amount_1_30days_SUM
    ,exp_sliding_bidist_count(ip_address, fraud_transaction_create_datetm, 30, 'days', account_id) as account_id_ip_address_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_email, fraud_transaction_create_datetm, 30, 'days', account_id) as account_id_account_email_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_password, fraud_transaction_create_datetm, 30, 'days', account_id) as account_id_account_password_30days_BI_COUNT
    ,exp_sliding_bidist_count(traveler_email_address_1, fraud_transaction_create_datetm, 30, 'days', account_id) as account_id_traveler_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_email_address_1, fraud_transaction_create_datetm, 30, 'days', account_id) as account_id_payment_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_phone_number_1, fraud_transaction_create_datetm, 30, 'days', account_id) as account_id_payment_phone_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(tsatravelid_1, fraud_transaction_create_datetm, 30, 'days', account_id) as account_id_tsatravelid_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(frequentflyernumber_1, fraud_transaction_create_datetm, 30, 'days', account_id) as account_id_frequentflyernumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(passportnumber_1, fraud_transaction_create_datetm, 30, 'days', account_id) as account_id_passportnumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_hashed_credit_card_number_1, fraud_transaction_create_datetm, 30, 'days', account_id) as account_id_payment_hashed_credit_card_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(item_destination_1, fraud_transaction_create_datetm, 30, 'days', account_id) as account_id_item_destination_1_30days_BI_COUNT
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where account_id is not null and account_id != ''
    distribute by account_id
    sort by account_id, fraud_transaction_create_datetm
  ) subq
) j_account_id on j_account_id.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', item_destination_1) as item_destination_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', item_destination_1)/10000.0 as double) as item_destination_1_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where item_destination_1 is not null and item_destination_1 != ''
    distribute by item_destination_1
    sort by item_destination_1, fraud_transaction_create_datetm
  ) subq
) j_item_destination_1 on j_item_destination_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', account_home_phone_area_code) as account_home_phone_area_code_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', account_home_phone_area_code)/10000.0 as double) as account_home_phone_area_code_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where account_home_phone_area_code is not null and account_home_phone_area_code != ''
    distribute by account_home_phone_area_code
    sort by account_home_phone_area_code, fraud_transaction_create_datetm
  ) subq
) j_account_home_phone_area_code on j_account_home_phone_area_code.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', item_airport_drop_off_location_1) as item_airport_drop_off_location_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', item_airport_drop_off_location_1)/10000.0 as double) as item_airport_drop_off_location_1_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where item_airport_drop_off_location_1 is not null and item_airport_drop_off_location_1 != ''
    distribute by item_airport_drop_off_location_1
    sort by item_airport_drop_off_location_1, fraud_transaction_create_datetm
  ) subq
) j_item_airport_drop_off_location_1 on j_item_airport_drop_off_location_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_country) as ip_country_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_country)/10000.0 as double) as ip_country_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_country is not null and ip_country != ''
    distribute by ip_country
    sort by ip_country, fraud_transaction_create_datetm
  ) subq
) j_ip_country on j_ip_country.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', transaction_guid) as transaction_guid_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', transaction_guid)/10000.0 as double) as transaction_guid_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where transaction_guid is not null and transaction_guid != ''
    distribute by transaction_guid
    sort by transaction_guid, fraud_transaction_create_datetm
  ) subq
) j_transaction_guid on j_transaction_guid.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_address_asn) as ip_address_asn_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_address_asn)/10000.0 as double) as ip_address_asn_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_address_asn is not null and ip_address_asn != ''
    distribute by ip_address_asn
    sort by ip_address_asn, fraud_transaction_create_datetm
  ) subq
) j_ip_address_asn on j_ip_address_asn.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_time_zone) as ip_time_zone_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_time_zone)/10000.0 as double) as ip_time_zone_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_time_zone is not null and ip_time_zone != ''
    distribute by ip_time_zone
    sort by ip_time_zone, fraud_transaction_create_datetm
  ) subq
) j_ip_time_zone on j_ip_time_zone.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_area_code) as ip_area_code_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_area_code)/10000.0 as double) as ip_area_code_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_area_code is not null and ip_area_code != ''
    distribute by ip_area_code
    sort by ip_area_code, fraud_transaction_create_datetm
  ) subq
) j_ip_area_code on j_ip_area_code.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', loyalty_member_id) as loyalty_member_id_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', loyalty_member_id)/10000.0 as double) as loyalty_member_id_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where loyalty_member_id is not null and loyalty_member_id != ''
    distribute by loyalty_member_id
    sort by loyalty_member_id, fraud_transaction_create_datetm
  ) subq
) j_loyalty_member_id on j_loyalty_member_id.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', payment_hashed_credit_card_number_1) as payment_hashed_credit_card_number_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', payment_hashed_credit_card_number_1)/10000.0 as double) as payment_hashed_credit_card_number_1_payment_usd_amount_1_30days_SUM
    ,exp_sliding_bidist_count(ip_address, fraud_transaction_create_datetm, 30, 'days', payment_hashed_credit_card_number_1) as payment_hashed_credit_card_number_1_ip_address_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_email, fraud_transaction_create_datetm, 30, 'days', payment_hashed_credit_card_number_1) as payment_hashed_credit_card_number_1_account_email_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_id, fraud_transaction_create_datetm, 30, 'days', payment_hashed_credit_card_number_1) as payment_hashed_credit_card_number_1_account_id_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_password, fraud_transaction_create_datetm, 30, 'days', payment_hashed_credit_card_number_1) as payment_hashed_credit_card_number_1_account_password_30days_BI_COUNT
    ,exp_sliding_bidist_count(traveler_email_address_1, fraud_transaction_create_datetm, 30, 'days', payment_hashed_credit_card_number_1) as payment_hashed_credit_card_number_1_traveler_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_email_address_1, fraud_transaction_create_datetm, 30, 'days', payment_hashed_credit_card_number_1) as payment_hashed_credit_card_number_1_payment_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_phone_number_1, fraud_transaction_create_datetm, 30, 'days', payment_hashed_credit_card_number_1) as payment_hashed_credit_card_number_1_payment_phone_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(tsatravelid_1, fraud_transaction_create_datetm, 30, 'days', payment_hashed_credit_card_number_1) as payment_hashed_credit_card_number_1_tsatravelid_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(frequentflyernumber_1, fraud_transaction_create_datetm, 30, 'days', payment_hashed_credit_card_number_1) as payment_hashed_credit_card_number_1_frequentflyernumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(passportnumber_1, fraud_transaction_create_datetm, 30, 'days', payment_hashed_credit_card_number_1) as payment_hashed_credit_card_number_1_passportnumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(item_destination_1, fraud_transaction_create_datetm, 30, 'days', payment_hashed_credit_card_number_1) as payment_hashed_credit_card_number_1_item_destination_1_30days_BI_COUNT
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where payment_hashed_credit_card_number_1 is not null and payment_hashed_credit_card_number_1 != ''
    distribute by payment_hashed_credit_card_number_1
    sort by payment_hashed_credit_card_number_1, fraud_transaction_create_datetm
  ) subq
) j_payment_hashed_credit_card_number_1 on j_payment_hashed_credit_card_number_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', payment_phone_number_1) as payment_phone_number_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', payment_phone_number_1)/10000.0 as double) as payment_phone_number_1_payment_usd_amount_1_30days_SUM
    ,exp_sliding_bidist_count(ip_address, fraud_transaction_create_datetm, 30, 'days', payment_phone_number_1) as payment_phone_number_1_ip_address_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_email, fraud_transaction_create_datetm, 30, 'days', payment_phone_number_1) as payment_phone_number_1_account_email_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_id, fraud_transaction_create_datetm, 30, 'days', payment_phone_number_1) as payment_phone_number_1_account_id_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_password, fraud_transaction_create_datetm, 30, 'days', payment_phone_number_1) as payment_phone_number_1_account_password_30days_BI_COUNT
    ,exp_sliding_bidist_count(traveler_email_address_1, fraud_transaction_create_datetm, 30, 'days', payment_phone_number_1) as payment_phone_number_1_traveler_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_email_address_1, fraud_transaction_create_datetm, 30, 'days', payment_phone_number_1) as payment_phone_number_1_payment_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(tsatravelid_1, fraud_transaction_create_datetm, 30, 'days', payment_phone_number_1) as payment_phone_number_1_tsatravelid_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(frequentflyernumber_1, fraud_transaction_create_datetm, 30, 'days', payment_phone_number_1) as payment_phone_number_1_frequentflyernumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(passportnumber_1, fraud_transaction_create_datetm, 30, 'days', payment_phone_number_1) as payment_phone_number_1_passportnumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_hashed_credit_card_number_1, fraud_transaction_create_datetm, 30, 'days', payment_phone_number_1) as payment_phone_number_1_payment_hashed_credit_card_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(item_destination_1, fraud_transaction_create_datetm, 30, 'days', payment_phone_number_1) as payment_phone_number_1_item_destination_1_30days_BI_COUNT
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where payment_phone_number_1 is not null and payment_phone_number_1 != ''
    distribute by payment_phone_number_1
    sort by payment_phone_number_1, fraud_transaction_create_datetm
  ) subq
) j_payment_phone_number_1 on j_payment_phone_number_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', traveler_email_address_1) as traveler_email_address_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', traveler_email_address_1)/10000.0 as double) as traveler_email_address_1_payment_usd_amount_1_30days_SUM
    ,exp_sliding_bidist_count(ip_address, fraud_transaction_create_datetm, 30, 'days', traveler_email_address_1) as traveler_email_address_1_ip_address_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_email, fraud_transaction_create_datetm, 30, 'days', traveler_email_address_1) as traveler_email_address_1_account_email_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_id, fraud_transaction_create_datetm, 30, 'days', traveler_email_address_1) as traveler_email_address_1_account_id_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_password, fraud_transaction_create_datetm, 30, 'days', traveler_email_address_1) as traveler_email_address_1_account_password_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_email_address_1, fraud_transaction_create_datetm, 30, 'days', traveler_email_address_1) as traveler_email_address_1_payment_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_phone_number_1, fraud_transaction_create_datetm, 30, 'days', traveler_email_address_1) as traveler_email_address_1_payment_phone_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(tsatravelid_1, fraud_transaction_create_datetm, 30, 'days', traveler_email_address_1) as traveler_email_address_1_tsatravelid_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(frequentflyernumber_1, fraud_transaction_create_datetm, 30, 'days', traveler_email_address_1) as traveler_email_address_1_frequentflyernumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(passportnumber_1, fraud_transaction_create_datetm, 30, 'days', traveler_email_address_1) as traveler_email_address_1_passportnumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_hashed_credit_card_number_1, fraud_transaction_create_datetm, 30, 'days', traveler_email_address_1) as traveler_email_address_1_payment_hashed_credit_card_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(item_destination_1, fraud_transaction_create_datetm, 30, 'days', traveler_email_address_1) as traveler_email_address_1_item_destination_1_30days_BI_COUNT
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where traveler_email_address_1 is not null and traveler_email_address_1 != ''
    distribute by traveler_email_address_1
    sort by traveler_email_address_1, fraud_transaction_create_datetm
  ) subq
) j_traveler_email_address_1 on j_traveler_email_address_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_address) as ip_address_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_address)/10000.0 as double) as ip_address_payment_usd_amount_1_30days_SUM
    ,exp_sliding_bidist_count(account_email, fraud_transaction_create_datetm, 30, 'days', ip_address) as ip_address_account_email_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_id, fraud_transaction_create_datetm, 30, 'days', ip_address) as ip_address_account_id_30days_BI_COUNT
    ,exp_sliding_bidist_count(account_password, fraud_transaction_create_datetm, 30, 'days', ip_address) as ip_address_account_password_30days_BI_COUNT
    ,exp_sliding_bidist_count(traveler_email_address_1, fraud_transaction_create_datetm, 30, 'days', ip_address) as ip_address_traveler_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_email_address_1, fraud_transaction_create_datetm, 30, 'days', ip_address) as ip_address_payment_email_address_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_phone_number_1, fraud_transaction_create_datetm, 30, 'days', ip_address) as ip_address_payment_phone_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(tsatravelid_1, fraud_transaction_create_datetm, 30, 'days', ip_address) as ip_address_tsatravelid_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(frequentflyernumber_1, fraud_transaction_create_datetm, 30, 'days', ip_address) as ip_address_frequentflyernumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(passportnumber_1, fraud_transaction_create_datetm, 30, 'days', ip_address) as ip_address_passportnumber_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(payment_hashed_credit_card_number_1, fraud_transaction_create_datetm, 30, 'days', ip_address) as ip_address_payment_hashed_credit_card_number_1_30days_BI_COUNT
    ,exp_sliding_bidist_count(item_destination_1, fraud_transaction_create_datetm, 30, 'days', ip_address) as ip_address_item_destination_1_30days_BI_COUNT
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_address is not null and ip_address != ''
    distribute by ip_address
    sort by ip_address, fraud_transaction_create_datetm
  ) subq
) j_ip_address on j_ip_address.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', vendorcode_1) as vendorcode_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', vendorcode_1)/10000.0 as double) as vendorcode_1_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where vendorcode_1 is not null and vendorcode_1 != ''
    distribute by vendorcode_1
    sort by vendorcode_1, fraud_transaction_create_datetm
  ) subq
) j_vendorcode_1 on j_vendorcode_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_second_level_domain) as ip_second_level_domain_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_second_level_domain)/10000.0 as double) as ip_second_level_domain_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_second_level_domain is not null and ip_second_level_domain != ''
    distribute by ip_second_level_domain
    sort by ip_second_level_domain, fraud_transaction_create_datetm
  ) subq
) j_ip_second_level_domain on j_ip_second_level_domain.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', payment_bin_number_1) as payment_bin_number_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', payment_bin_number_1)/10000.0 as double) as payment_bin_number_1_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where payment_bin_number_1 is not null and payment_bin_number_1 != ''
    distribute by payment_bin_number_1
    sort by payment_bin_number_1, fraud_transaction_create_datetm
  ) subq
) j_payment_bin_number_1 on j_payment_bin_number_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', account_home_phone_number) as account_home_phone_number_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', account_home_phone_number)/10000.0 as double) as account_home_phone_number_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where account_home_phone_number is not null and account_home_phone_number != ''
    distribute by account_home_phone_number
    sort by account_home_phone_number, fraud_transaction_create_datetm
  ) subq
) j_account_home_phone_number on j_account_home_phone_number.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', item_airports_1) as item_airports_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', item_airports_1)/10000.0 as double) as item_airports_1_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where item_airports_1 is not null and item_airports_1 != ''
    distribute by item_airports_1
    sort by item_airports_1, fraud_transaction_create_datetm
  ) subq
) j_item_airports_1 on j_item_airports_1.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', accountphonenum) as accountphonenum_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', accountphonenum)/10000.0 as double) as accountphonenum_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where accountphonenum is not null and accountphonenum != ''
    distribute by accountphonenum
    sort by accountphonenum, fraud_transaction_create_datetm
  ) subq
) j_accountphonenum on j_accountphonenum.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_reg_org) as ip_reg_org_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_reg_org)/10000.0 as double) as ip_reg_org_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_reg_org is not null and ip_reg_org != ''
    distribute by ip_reg_org
    sort by ip_reg_org, fraud_transaction_create_datetm
  ) subq
) j_ip_reg_org on j_ip_reg_org.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', ip_state) as ip_state_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', ip_state)/10000.0 as double) as ip_state_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where ip_state is not null and ip_state != ''
    distribute by ip_state
    sort by ip_state, fraud_transaction_create_datetm
  ) subq
) j_ip_state on j_ip_state.fraud_transaction_id=src.fraud_transaction_id


left outer join (
  select
    fraud_transaction_id
    ,exp_sliding_count(fraud_transaction_create_datetm, 30, 'days', item_hotel_id_1) as item_hotel_id_1_30days_COUNT
    ,cast(exp_sliding_sum( cast(cast(payment_usd_amount_1 as double) * 10000 as bigint), fraud_transaction_create_datetm, 30, 'days', item_hotel_id_1)/10000.0 as double) as item_hotel_id_1_payment_usd_amount_1_30days_SUM
  from (
    select
      cast(fraud_transaction_id as int) as fraud_transaction_id
      ,cast(fraud_transaction_create_datetm as timestamp) as fraud_transaction_create_datetm
      ,account_home_phone_country_access_code
      ,ip_continent
      ,accountnum
      ,ip_address_cidr
      ,ip_top_level_domain
      ,account_company_name
      ,account_person_name
      ,frequentflyernumber_1
      ,ip_city
      ,tsatravelid_1
      ,ip_postal_code
      ,item_postal_code_1
      ,account_password
      ,payment_last_name_1
      ,payment_email_address_1
      ,ip_routing_method
      ,traveler_full_name_1
      ,item_airport_pick_up_location_1
      ,account_email
      ,payment_issue_country_1
      ,passportnumber_1
      ,item_city_1
      ,item_vendor_name_1
      ,machine_id
      ,payment_usd_amount_1
      ,ip_carrier
      ,account_id
      ,item_destination_1
      ,account_home_phone_area_code
      ,item_airport_drop_off_location_1
      ,ip_country
      ,transaction_guid
      ,ip_address_asn
      ,ip_time_zone
      ,ip_area_code
      ,loyalty_member_id
      ,payment_hashed_credit_card_number_1
      ,payment_phone_number_1
      ,traveler_email_address_1
      ,ip_address
      ,vendorcode_1
      ,ip_second_level_domain
      ,payment_bin_number_1
      ,account_home_phone_number
      ,item_airports_1
      ,accountphonenum
      ,ip_reg_org
      ,ip_state
      ,item_hotel_id_1
    from gfora.${SRC_TABLE}
    where item_hotel_id_1 is not null and item_hotel_id_1 != ''
    distribute by item_hotel_id_1
    sort by item_hotel_id_1, fraud_transaction_create_datetm
  ) subq
) j_item_hotel_id_1 on j_item_hotel_id_1.fraud_transaction_id=src.fraud_transaction_id
; 
