from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

level = "2" #Какую таблицу тестируем, маленькую, среднюю или большую
your_bucket_name = "result2v" #Имя вашего бакета
your_access_key = "MWAHF8M9M01TTWHP8TR6" #Ключ от вашего бакета
your_secret_key = "551ZN3UDsGL5yZJWBt0Q4cP32m5ZquYjtq7XenL8" #Ключ от вашего бакета

configs = {
    "spark.sql.files.maxPartitionBytes": "1073741824", #1GB
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.block.size": "134217728", # 128MB
    "spark.hadoop.fs.s3a.multipart.size": "268435456", # 256MB
    "spark.hadoop.fs.s3a.multipart.threshold": "536870912", # 512MB
    "spark.hadoop.fs.s3a.committer.name": "magic",
    "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled": "true",
    "spark.hadoop.fs.s3a.threads.max": "64",
    "spark.hadoop.fs.s3a.connection.maximum": "64",
    "spark.hadoop.fs.s3a.fast.upload.buffer": "array",
    "spark.hadoop.fs.s3a.directory.marker.retention": "keep",
    "spark.hadoop.fs.s3a.endpoint": "api.s3.az1.t1.cloud",
    "spark.hadoop.fs.s3a.bucket.source-data.access.key": "P2EGND58XBW5ASXMYLLK",
    "spark.hadoop.fs.s3a.bucket.source-data.secret.key": "IDkOoR8KKmCuXc9eLAnBFYDLLuJ3NcCAkGFghCJI",
    f"spark.hadoop.fs.s3a.bucket.{your_bucket_name}.access.key": your_access_key,
    f"spark.hadoop.fs.s3a.bucket.{your_bucket_name}.secret.key": your_secret_key,
    "spark.sql.parquet.compression.codec": "zstd"
}
conf = SparkConf()
conf.setAll(configs.items())

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
log = spark._jvm.org.apache.log4j.LogManager.getLogger(">>> App")

incr_bucket = f"s3a://source-data"
your_bucket = f"s3a://{your_bucket_name}"
incr_table = f"{incr_bucket}/incr{level}"  # таблица с источника ODS , куда мы скопировали инкремент
table_to_copy = f"init{level}"
init_table = f"{your_bucket}/{table_to_copy}"
hist_table = f"{your_bucket}/{table_to_copy}.history"
ver_table = f"{your_bucket}/{table_to_copy}.versions"
commitedVer_table = f"{your_bucket}/{table_to_copy}.versions.commited"

version=spark.read.parquet(commitedVer_table).sort(desc("_DL_version")).head(1)[0]._DL_version
nextVersion=version+1
log.info(f"Last version: {version}")
newVersion=spark.createDataFrame([{"_DL_version":nextVersion}])
newVersion.write.mode("append").parquet(ver_table)

incr_data=spark.read.parquet(incr_table)
history=incr_data.filter(col("eff_to_dt") != lit("5999-12-31"))
now=incr_data.filter(col("eff_to_dt") == lit("5999-12-31"))

log.info("Writing unchanged")
spark.read.parquet(init_table).filter(col("_DL_version") == version). \
    withColumn("eff_from_month", last_day(col("eff_from_dt")).cast(StringType())). \
    withColumn("eff_to_month", last_day(col("eff_to_dt")).cast(StringType())). \
    withColumn("_DL_version",col("_DL_version")+1). \
    join(now, on="id", how="left_anti"). \
    write.mode("append").partitionBy("_DL_version", "eff_from_month").parquet(init_table)
log.info("Writing changed")
now. \
    withColumn("eff_from_month", last_day(col("eff_from_dt")).cast(StringType())). \
    withColumn("eff_to_month", last_day(col("eff_to_dt")).cast(StringType())). \
    join(newVersion, [nextVersion==newVersion._DL_version], "left").write.mode("append"). \
    partitionBy("_DL_version", "eff_from_month").parquet(init_table)
log.info("Writing changes")
history. \
    withColumn("eff_from_month", last_day(col("eff_from_dt")).cast(StringType())). \
    withColumn("eff_to_month", last_day(col("eff_to_dt")).cast(StringType())). \
    join(newVersion, [nextVersion==newVersion._DL_version], "left").write.mode("append"). \
    partitionBy("eff_to_month", "eff_from_month", "_DL_version").parquet(hist_table)

log.info("Commit version")
newVersion.write.mode("append").parquet(commitedVer_table)

log.info("Finished")
