# SPARK Pyspark Script

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import date, timedelta, datetime
from pytz import timezone
import boto3

#Date Parameter
DATE_FLAG = datetime.now(timezone('Asia/Seoul'))-timedelta(1)

#Date to String
DATE_PARAM = DATE_FLAG.strftime('%Y%m%d')
DATE_PARAM_1 = (DATE_FLAG-timedelta(1)).strftime('%Y%m%d')
DATE_PARAM_7 = (DATE_FLAG-timedelta(7)).strftime('%Y%m%d')

USE_DATE = DATE_FLAG.strftime('%Y-%m-%d')
DATE_YEAR = DATE_FLAG.strftime('%Y')[2:4]
DATE_MONTH = DATE_FLAG.strftime('%m')

MONTH_CHECK = DATE_YEAR+DATE_MONTH+"m"

#Read Data Path
S3_PATH_A = "S3 PATH/{0}".format(DATE_PARAM_1)
S3_PATH_E = "S3 PATH/{0}".format(DATE_PARAM)
S3_PATH_B = "S3 PATH/{0}".format(DATE_PARAM)

#Write Data Path
WRITE_DATA_PATH = "S3 PATH/{0}".format(DATE_PARAM)

#Partition Config
# EMR Cluster
# - master: r4.xlarge
# - core: r4.8xlarge x 6
NUM_CORE = 188
NUM_PARTITION = NUM_CORE*2

#SparkSession
spark = SparkSession.builder.appName("ETL Application 2").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", NUM_PARTITION)

#Read Data
read_adid_info = spark.read.parquet(S3_PATH_E)\
                           .select("adid", "untracked_date")

untracked_null_adid = read_adid_info.filter("untracked_date is null").select("adid")

datetime_slice = udf(lambda datetime: datetime[:8]) #UDF
read_app_tracking = spark.read.parquet(S3_PATH_B)\
                              .select("adid", "package_name", "appid", "install_market",
                                      "first_install_datetime", "last_update_datetime", "last_use_datetime", "daily_use_sec")\
                              .distinct()\
                              .withColumn("first_install_datetime", regexp_replace("first_install_datetime", "\D", "0"))\
                              .withColumn("last_update_datetime", regexp_replace("last_update_datetime", "\D", "0"))\
                              .withColumn("last_use_datetime", regexp_replace("last_use_datetime", "\D", "0"))\
                              .replace(["UNKNOWN"], ["NA"], ["install_market"])\
                              .withColumn("first_install_date", datetime_slice(col("first_install_datetime")).cast(LongType()))\
                              .withColumn("last_update_date", datetime_slice(col("last_update_datetime")).cast(LongType()))\
                              .withColumn("last_use_date", datetime_slice(col("last_use_datetime")).cast(LongType()))

valid_app_tracking = read_app_tracking.groupBy("adid", "package_name", "appid", "install_market")\
                                      .agg(max(col("first_install_date")).cast(StringType()).alias("first_install_date"),
                                           max(col("last_update_date")).cast(StringType()).alias("last_update_date"),
                                           max(col("last_use_date")).cast(StringType()).alias("max_last_use_date"),
                                           max(col("daily_use_sec")).alias("cum_use_sec"))\
                                      .withColumn("last_install_date", lit(DATE_PARAM))\
                                      .select("adid", "package_name", "appid", "install_market", "first_install_date", "last_install_date", "last_update_date", "max_last_use_date", "cum_use_sec")\
                                      .replace(["0"], ["00000000"], ["first_install_date", "last_update_date", "max_last_use_date"])

week_check = udf(lambda x: str((int(DATE_YEAR)+1))+x+"w" if (DATE_MONTH == '12' and x == '1') else DATE_YEAR+x+"w" if x else None) #UDF
current_package_use_history = untracked_null_adid.join(valid_app_tracking, untracked_null_adid.adid == valid_app_tracking.adid, "left")\
                                                 .withColumn("last_use_date", when(expr("cum_use_sec > 0"), lit(DATE_PARAM_1)).otherwise(col("max_last_use_date")))\
                                                 .withColumn("use_date", when(expr("cum_use_sec >= 3"), lit(USE_DATE)))\
                                                 .withColumn("use_week", week_check(weekofyear(col("use_date")).cast(StringType())))\
                                                 .withColumn("use_month", when(col("use_date").isNotNull(), lit(MONTH_CHECK)))\
                                                 .select(untracked_null_adid.adid, "package_name", "appid", "install_market", "first_install_date", "last_install_date", "last_update_date", "last_use_date", "cum_use_sec",
                                                         "use_week", "use_month")

split_use_date = udf(lambda x : x.split('|')[-1] if x else None) #UDF
past_package_use_history = spark.read.parquet(S3_PATH_A)\
                                     .withColumn("last_week_check", split_use_date(col("use_week")))\
                                     .withColumn("last_month_check", split_use_date(col("use_month")))

#Join
cond = [past_package_use_history.adid == current_package_use_history.adid,
        past_package_use_history.package_name == current_package_use_history.package_name,
        past_package_use_history.install_market == current_package_use_history.install_market]

outer_package_use_history = past_package_use_history.join(current_package_use_history, cond, "outer")\
                                                    .select(past_package_use_history.adid.alias("p_adid"),
                                                            current_package_use_history.adid.alias("c_adid"),
                                                            past_package_use_history.package_name.alias("p_package_name"),
                                                            current_package_use_history.package_name.alias("c_package_name"),
                                                            past_package_use_history.appid.alias("p_appid"),
                                                            current_package_use_history.appid.alias("c_appid"),
                                                            past_package_use_history.install_market.alias("p_install_market"),
                                                            current_package_use_history.install_market.alias("c_install_market"),
                                                            past_package_use_history.first_install_date.alias("p_first_install_date"),
                                                            current_package_use_history.first_install_date.alias("c_first_install_date"),
                                                            past_package_use_history.last_install_date.alias("p_last_install_date"),
                                                            current_package_use_history.last_install_date.alias("c_last_install_date"),
                                                            past_package_use_history.last_delete_date.alias("p_last_delete_date"),
                                                            past_package_use_history.reinstall_cnt.alias("p_reinstall_cnt"),
                                                            past_package_use_history.last_update_date.alias("p_last_update_date"),
                                                            current_package_use_history.last_update_date.alias("c_last_update_date"),
                                                            past_package_use_history.last_use_date.alias("p_last_use_date"),
                                                            current_package_use_history.last_use_date.alias("c_last_use_date"),
                                                            past_package_use_history.cum_use_sec.alias("p_cum_use_sec"),
                                                            current_package_use_history.cum_use_sec.alias("c_cum_use_sec"),
                                                            past_package_use_history.active_flag.alias("p_active_flag"),
                                                            past_package_use_history.use_week,
                                                            past_package_use_history.use_month,
                                                            past_package_use_history.last_week_check,
                                                            past_package_use_history.last_month_check,
                                                            current_package_use_history.use_week.alias("c_use_week"),
                                                            current_package_use_history.use_month.alias("c_use_month"))\
                                                    .persist()

#add
add_package_use_history = outer_package_use_history.filter("p_adid is null and p_package_name is null")\
                                                   .withColumn("reinstall_cnt", lit(0))\
                                                   .withColumn("active_flag", lit(True).cast(BooleanType()))\
                                                   .select("c_adid", "c_package_name", "c_appid", "c_install_market", 
                                                           "c_first_install_date", "c_last_install_date", "p_last_delete_date", "reinstall_cnt", 
                                                           "c_last_update_date", "c_last_use_date", "c_cum_use_sec", "active_flag",
                                                           "use_week", "use_month", "last_week_check", "last_month_check", "c_use_week", "c_use_month")

#delete filter
delete_package_use_history_filter = outer_package_use_history.filter("c_adid is null and c_package_name is null")\
                                                             .select("p_adid", "p_package_name", "p_appid", "p_install_market", 
                                                                     "p_first_install_date", "p_last_install_date", "p_reinstall_cnt", 
                                                                     "p_last_update_date", "p_last_use_date", "p_cum_use_sec",
                                                                     "p_last_delete_date", "p_active_flag",
                                                                     "use_week", "use_month", "last_week_check", "last_month_check", "c_use_week", "c_use_month")

#delete - adid hold untracked_date in 7 days
delete_package_use_history = read_adid_info.join(delete_package_use_history_filter, read_adid_info.adid == delete_package_use_history_filter.p_adid, "inner")\
                                           .withColumn("last_delete_date", when(expr("untracked_date is not null and CAST(untracked_date AS INT) >= {0}".format(DATE_PARAM_7)), col("p_last_delete_date"))
                                                                .otherwise(when(expr("p_active_flag = false"), col("p_last_delete_date"))
                                                                .otherwise(lit(DATE_PARAM))))\
                                           .withColumn("active_flag",  when(expr("untracked_date is not null and CAST(untracked_date AS INT) >= {0}".format(DATE_PARAM_7)), col("p_active_flag"))
                                                                  .otherwise(lit(False).cast(BooleanType())))\
                                           .select("p_adid", "p_package_name", "p_appid", "p_install_market",
                                                   "p_first_install_date", "p_last_install_date", "last_delete_date", "p_reinstall_cnt",
                                                   "p_last_update_date", "p_last_use_date", "p_cum_use_sec", "active_flag",
                                                   "use_week", "use_month", "last_week_check", "last_month_check", "c_use_week", "c_use_month")


#update
update_package_use_history = outer_package_use_history.filter("p_adid is not null and p_package_name is not null and c_adid is not null and c_package_name is not null")\
                                                      .withColumn("appid", when((col("p_appid") != "NA") & (col("c_appid") == "NA"), col("p_appid")).otherwise(col("c_appid")))\
                                                      .withColumn("first_install_date", when((col("p_first_install_date").cast(IntegerType()) != 0) & (col("c_first_install_date").cast(IntegerType()) == 0), col("p_first_install_date"))
                                                                             .otherwise(when(col("p_first_install_date").cast(IntegerType()) > col("c_first_install_date").cast(IntegerType()), col("p_first_install_date")).otherwise(col("c_first_install_date"))))\
                                                      .withColumn("last_update_date", when((col("p_last_update_date").cast(IntegerType()) != 0) & (col("c_last_update_date").cast(IntegerType()) == 0), col("p_last_update_date"))
                                                                           .otherwise(when(col("p_last_update_date").cast(IntegerType()) > col("c_last_update_date").cast(IntegerType()), col("p_last_update_date")).otherwise(col("c_last_update_date"))))\
                                                      .withColumn("last_use_date", when((col("p_last_use_date").cast(IntegerType()) != 0) & (col("c_last_use_date").cast(IntegerType()) == 0), col("p_last_use_date"))
                                                                          .otherwise(when(col("p_last_use_date").cast(IntegerType()) > col("c_last_use_date").cast(IntegerType()), col("p_last_use_date")).otherwise(col("c_last_use_date"))))\
                                                      .withColumn("last_install_date", when((col("p_active_flag") == False), col("c_last_install_date")).otherwise(col("p_last_install_date")))\
                                                      .withColumn("reinstall_cnt", when((col("p_active_flag") == False), col("p_reinstall_cnt")+1).otherwise(col("p_reinstall_cnt")))\
                                                      .withColumn("cum_use_sec", (col("p_cum_use_sec")+col("c_cum_use_sec")))\
                                                      .withColumn("active_flag", lit(True).cast(BooleanType()))\
                                                      .select("c_adid", "c_package_name", "appid", "c_install_market", 
                                                              "first_install_date", "last_install_date", "p_last_delete_date", "reinstall_cnt", 
                                                              "last_update_date", "last_use_date", "cum_use_sec", "active_flag",
                                                              "use_week", "use_month", "last_week_check", "last_month_check", "c_use_week", "c_use_month")

#union
package_use_history = update_package_use_history.union(delete_package_use_history).union(add_package_use_history)\
                                                .withColumn("use_week", when(col("c_use_week").isNull(), col("use_week"))
                                                               .otherwise(when(col("c_use_week").isNotNull() & col("use_week").isNull(), col("c_use_week"))
                                                               .otherwise(when(col("c_use_week").isNotNull() & col("use_week").isNotNull(), when(col("c_use_week") == col("last_week_check"), col("use_week"))
                                                                                                                                                .otherwise(concat_ws('|', col("use_week"), col("c_use_week")))))))\
                                                .withColumn("use_month", when(col("c_use_month").isNull(), col("use_month"))
                                                               .otherwise(when(col("c_use_month").isNotNull() & col("use_month").isNull(), col("c_use_month"))
                                                               .otherwise(when(col("c_use_month").isNotNull() & col("use_month").isNotNull(), when(col("c_use_month") == col("last_month_check"), col("use_month"))
                                                                                                                                                .otherwise(concat_ws('|', col("use_month"), col("c_use_month")))))))\
                                                .selectExpr("c_adid as adid", "c_package_name as package_name", "appid", "c_install_market as install_market", 
                                                            "first_install_date", "last_install_date", "p_last_delete_date as last_delete_date", "reinstall_cnt", 
                                                            "last_update_date", "last_use_date", "cum_use_sec", "use_week", "use_month", "active_flag")\
                                                .orderBy("adid", "package_name")

#Write
package_use_history.coalesce(NUM_CORE).write.parquet(WRITE_DATA_PATH)

outer_package_use_history.unpersist()

#Athena table update
client = boto3.client('athena', region_name='')
client.start_query_execution(QueryString="QUERY".format(DATE_PARAM), QueryExecutionContext={'Database': ''}, ResultConfiguration={''})
