SET hive.stats.autogather=false;
SET hive.exec.compress.output=true; 
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec; 
SET mapred.output.compression.type=BLOCK;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

USE gfora;

DROP TABLE FINAL_TABLE_1;

CREATE TABLE FINAL_TABLE_1
ROW FORMAT DELIMITED
STORED AS SEQUENCEFILE
AS

SELECT
  a.fraudtransaction_fraudtransactionid
 ,a.fraudtransaction_iscompletedbooking
 ,a.fraudtransaction_trl
 ,c.ITEM_AMOUNT_LOCAL
 ,c.ITEM_AMOUNT_USD_DERIVED
 ,c.ITEM_CURRENCY_CODE
 ,c.PRODUCT_ITEM_COUNT
 ,c.TRAVELER_PHONE_NUMBER_COUNT
 ,c.TRAVELER_IS_ANY_GIVEN_NAME_LOWER_CASE
 ,c.TRAVELER_IS_ANY_GIVEN_NAME_UPPER_CASE
 ,c.TRAVELER_IS_ANY_SURNAME_LOWER_CASE
 ,c.TRAVELER_IS_ANY_SURNAME_UPPER_CASE
 ,d.PAYMENT_ITEM_COUNT
FROM
 (
 SELECT
   fraudtransaction_fraudtransactionid
  ,fraudtransaction_createdate
  ,max(fraudtransaction_iscompletedbooking) as fraudtransaction_iscompletedbooking
  ,max(fraudtransaction_trl) as fraudtransaction_trl
 FROM
  ETLDATA.FRAUD_ZEUS_FRAUDTRANSACTION_FLAT_DRV_BKT
 WHERE
  to_date(fraudtransaction_createdate) = '2013'
 GROUP BY
   fraudtransaction_fraudtransactionid
  ,fraudtransaction_createdate 
 ) a
INNER JOIN
 (
 SELECT
   FRAUD_TRANSACTION_ID
  ,sum(ITEM_AMOUNT) as ITEM_AMOUNT_LOCAL
  ,sum(ITEM_AMOUNT_USD_DERIVED) as ITEM_AMOUNT_USD_DERIVED
  ,max(ITEM_CURRENCY_CODE) as ITEM_CURRENCY_CODE
  ,max(PRODUCT_ITEM_COUNT) as PRODUCT_ITEM_COUNT
  ,count(ORDER_LINE_GUID) as PRODUCT_ITEM_COUNT_DERIVED
  ,sum(TRAVELER_PHONE_NUMBER_COUNT) as TRAVELER_PHONE_NUMBER_COUNT
  ,max(TRAVELER_IS_ANY_GIVEN_NAME_LOWER_CASE) as TRAVELER_IS_ANY_GIVEN_NAME_LOWER_CASE
  ,max(TRAVELER_IS_ANY_GIVEN_NAME_UPPER_CASE) as TRAVELER_IS_ANY_GIVEN_NAME_UPPER_CASE
  ,max(TRAVELER_IS_ANY_SURNAME_LOWER_CASE) as TRAVELER_IS_ANY_SURNAME_LOWER_CASE
  ,max(TRAVELER_IS_ANY_SURNAME_UPPER_CASE) as TRAVELER_IS_ANY_SURNAME_UPPER_CASE
 FROM
  gfora.FRAUD_SCORING_XML_MODEL_DETAIL_DRV1
 GROUP BY
  FRAUD_TRANSACTION_ID
 ) c
ON
a.fraudtransaction_fraudtransactionid = c.FRAUD_TRANSACTION_ID
INNER JOIN
 (
 SELECT
   FRAUD_TRANSACTION_ID
  ,max(PAYMENT_ITEM_COUNT) as PAYMENT_ITEM_COUNT
  ,max(PAYMENT_SEQUENCE_NUMBER) as PAYMENT_ITEM_COUNT_DERIVED
 FROM
  ETLDATA.FRAUD_SCORING_XML_PAYMENT_DETAIL
 WHERE
  substr(fraud_transaction_create_date,1,4)='2013' 
 GROUP BY 
  FRAUD_TRANSACTION_ID
) d
ON
a.fraudtransaction_fraudtransactionid = d.FRAUD_TRANSACTION_ID
;
