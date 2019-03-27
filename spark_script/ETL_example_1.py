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

#Read Data Path
S3_PATH_A = "S3 PATH/{0}".format(DATE_PARAM_1)
S3_PATH_B = "S3 PATH/{0}".format(DATE_PARAM)
S3_PATH_C = "S3 PATH/{0}".format(DATE_PARAM)
S3_PATH_D = "S3 PATH/{0}".format(DATE_PARAM)

#Write Data Path
WRITE_DATA_PATH = "S3 PATH/{0}".format(DATE_PARAM)

#Partition Config
NUM_CORE = 47
NUM_PARTITION = NUM_CORE*3

#SparkSession
spark = SparkSession.builder.appName("ETL Application 1").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", NUM_PARTITION)

#UDF
device_os_validation = udf(lambda x : x.strip() if x else None)

#Read Data
read_adid_info = spark.read.parquet(S3_PATH_B)\
                           .select("adid", "age", "gender", "carrier", "device_model", "device_os" )\
                           .repartition(NUM_PARTITION, "adid").distinct()\
                           .replace(["ZZ", "Z", "ZZZ"], ["NA", "NA", "NA"], ["age", "gender", "carrier"])\
                           .withColumn("device_os", device_os_validation(col("device_os")))\
                           .fillna("NA", subset=["device_model", "device_os"])\
                           .persist()

#Filtering issue data
issue_adid = read_adid_info.groupBy("adid").agg(count("adid")).filter("count(adid)>1").select("adid")
issue_adid_info = issue_adid.join(read_adid_info, issue_adid.adid == read_adid_info.adid, "left")\
                            .select(issue_adid.adid.alias("i_adid"), col("age").alias("i_age"), col("gender").alias("i_gender"), col("carrier").alias("i_carrier"), col("device_model").alias("i_device_model"), col("device_os").alias("i_device_os"))

current_adid_info = read_adid_info.subtract(issue_adid_info)
past_adid_info = spark.read.parquet(S3_PATH_A)

#Join
outer_adid_info = past_adid_info.join(current_adid_info, past_adid_info.adid == current_adid_info.adid, "outer")\
                                .select(past_adid_info.adid.alias("p_adid"),
                                        current_adid_info.adid.alias("c_adid"),
                                        past_adid_info.age.alias("p_age"),
                                        past_adid_info.p_age.alias("predicted_age"),
                                        current_adid_info.age.alias("c_age"),
                                        past_adid_info.gender.alias("p_gender"),
                                        past_adid_info.p_gender.alias("predicted_gender"),
                                        current_adid_info.gender.alias("c_gender"),
                                        past_adid_info.carrier.alias("p_carrier"),
                                        current_adid_info.carrier.alias("c_carrier"),
                                        past_adid_info.device_model.alias("p_device_model"),
                                        current_adid_info.device_model.alias("c_device_model"),
                                        past_adid_info.device_os.alias("p_device_os"),
                                        current_adid_info.device_os.alias("c_device_os"),
                                        past_adid_info.regist_date.alias("p_regist_date"),
                                        past_adid_info.update_date.alias("p_update_date"),
                                        past_adid_info.untracked_date.alias("p_untracked_date"))\
                                .persist()

#untracked
untracked_adid = outer_adid_info.filter("c_adid is null")\
                                .withColumn("untracked_date",  when((col("p_untracked_date").isNotNull()), col("p_untracked_date")).otherwise(lit(DATE_PARAM)))\
                                .select("p_adid", "p_age", "predicted_age", "p_gender", "predicted_gender", "p_carrier", "p_device_model", "p_device_os", "p_regist_date", "p_update_date", "untracked_date")

#add
add_adid = outer_adid_info.filter("p_adid is null")\
                          .selectExpr("c_adid as adid", "c_age", "c_gender", "c_carrier", "c_device_model", "c_device_os")\
                          .withColumn("regist_date", lit(DATE_PARAM))\
                          .withColumn("update_date", lit(DATE_PARAM))\
                          .withColumn("untracked_date", lit(None).cast(StringType()))

#demo predict
#age
age_func = udf(lambda x: "0"+x if len(x) == 1 else x)
age_predicted = spark.read.csv(S3_PATH_C)\
                           .withColumnRenamed("_c0", "adid")\
                           .withColumnRenamed("_c1", "predicted_age")\
                           .withColumn("predicted_age", age_func(col("predicted_age")))
#gender
gender_predicted = spark.read.csv(S3_PATH_D)\
                           .withColumnRenamed("_c0", "adid")\
                           .withColumnRenamed("_c1", "predicted_gender")

predicted_add_adid = add_adid.join(age_predicted, "adid", "left").join(gender_predicted, "adid", "left")\
                             .withColumn("predicted_age", when(col("predicted_age").isNull(), col("c_age")).otherwise(col("predicted_age")))\
                             .withColumn("predicted_gender", when(col("predicted_gender").isNull(), col("c_gender")).otherwise(col("predicted_gender")))\
                             .select("adid", "c_age", "predicted_age", "c_gender", "predicted_gender", "c_carrier", "c_device_model", "c_device_os", "regist_date", "update_date", "untracked_date")

#update
update_adid = outer_adid_info.filter("p_adid is not null and c_adid is not null")\
                             .withColumn("age", when((col("p_age") != "NA") & (col("c_age") == "NA"), col("p_age")).otherwise(col("c_age")))\
                             .withColumn("gender", when((col("p_gender") != "NA") & (col("c_gender") == "NA"), col("p_gender")).otherwise(col("c_gender")))\
                             .withColumn("carrier", when((col("p_carrier") != "NA") & (col("c_carrier") == "NA"), col("p_carrier")).otherwise(col("c_carrier")))\
                             .withColumn("device_model", when((col("p_device_model") != "NA") & (col("c_device_model") == "NA"), col("p_device_model")).otherwise(col("c_device_model")))\
                             .withColumn("device_os", when((col("p_device_os") != "NA") & (col("c_device_os") == "NA"), col("p_device_os")).otherwise(col("c_device_os")))\
                             .withColumn("age_flag", when((col("p_age") != "NA") & (col("c_age") == "NA"), 0).otherwise(when((col("p_age") == col("c_age")), 1).otherwise(2)))\
                             .withColumn("gender_flag", when((col("p_gender") != "NA") & (col("c_gender") == "NA"), 0).otherwise(when((col("p_gender") == col("c_gender")), 1).otherwise(2)))\
                             .withColumn("carrier_flag", when((col("p_carrier") != "NA") & (col("c_carrier") == "NA"), 0).otherwise(when((col("p_carrier") == col("c_carrier")), 1).otherwise(2)))\
                             .withColumn("device_model_flag", when((col("p_device_model") != "NA") & (col("c_device_model") == "NA"), 0).otherwise(when((col("p_device_model") == col("c_device_model")), 1).otherwise(2)))\
                             .withColumn("device_os_flag", when((col("p_device_os") != "NA") & (col("c_device_os") == "NA"), 0).otherwise(when((col("p_device_os") == col("c_device_os")), 1).otherwise(2)))\
                             .withColumn("update_date", when((col("age_flag") == 2) | (col("gender_flag") == 2) | (col("carrier_flag") == 2) | (col("device_model_flag") == 2) | (col("device_os_flag") == 2), lit(DATE_PARAM)).otherwise(col("p_update_date")))\
                             .withColumn("untracked_date", lit(None).cast(StringType()))\
                             .withColumn("p_age", when((col("age") != "NA") & (col("gender") != "NA"), col("age")).otherwise(col("predicted_age")))\
                             .withColumn("p_gender", when((col("age") != "NA") & (col("gender") != "NA"), col("gender")).otherwise(col("predicted_gender")))\
                             .select(col("p_adid").alias("adid"), "age", "p_age", "gender", "p_gender", "carrier", "device_model", "device_os", col("p_regist_date").alias("regist_date"), "update_date", "untracked_date")

#Union
adid_info = update_adid.union(untracked_adid).union(predicted_add_adid)

#Wrtie
adid_info.orderBy("adid", "regist_date").coalesce(NUM_CORE).write.mode("overwrite").parquet(WRITE_DATA_PATH)

read_adid_info.unpersist()
outer_adid_info.unpersist()

#Athena table updata
client = boto3.client('athena', region_name='')
client.start_query_execution(QueryString="QUERY".format(DATE_PARAM),
                             QueryExecutionContext={'Database': ''}, ResultConfiguration={'OutputLocation':''})
