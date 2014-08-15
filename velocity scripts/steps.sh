set -o errexit


START_DATE='2013-01-01'
END_DATE='2013-12-31' 


#END_DATE='2013-01-01'


TMP_JOIN_TABLE=tmp_fraud_weighted_scorecard_joins
TMP_VEL_TABLE=tmp_velocity_v3
FINAL_TABLE=fraud_weighted_scorecard_flat_source_v5


echo "START_DATE: $START_DATE"
echo "END_DATE  : $END_DATE"
echo "-----------------------"
echo "TMP_JOIN_TABLE: $TMP_JOIN_TABLE"
echo "TMP_VEL_TABLE: $TMP_VEL_TABLE"
echo "FINAL_TABLE: $FINAL_TABLE"
echo "-----------------------"
echo "Running: 10_load_from_source.hql ..."
hive -d "START_DATE=$START_DATE" -d "END_DATE=$END_DATE" -d "TARGET_TABLE=$TMP_JOIN_TABLE" -f 10_load_from_source.hql


# Run velocity calculation
echo "Running: 20_calculate_velocity.hql ..."
hive -d "START_DATE=$START_DATE" -d "END_DATE=$END_DATE" -d "SRC_TABLE=$TMP_JOIN_TABLE" -d "TARGET_TABLE=$TMP_VEL_TABLE" -f 20_calculate_velocity.hql




#Join with Velocity
echo "Running: 20_calculate_velocity.hql ..."
hive -d "START_DATE=$START_DATE" -d "END_DATE=$END_DATE" -d "SRC_TABLE=$TMP_JOIN_TABLE" -d "VEL_TABLE=$TMP_VEL_TABLE" -d "TARGET_TABLE=$FINAL_TABLE" -f 30_join_velocity.hql


echo "Set permissions on the target folder"
hadoop fs -chmod -R 777 "/user/hive/warehouse/platdev.db/$FINAL_TABLE"


# echo "Running: 40_drop_tmp.hql ..."
hive -d "TMP_JOIN_TABLE=$TMP_JOIN_TABLE" -d "TMP_VEL_TABLE=$TMP_VEL_TABLE" -f 40_drop_tmp.hql

