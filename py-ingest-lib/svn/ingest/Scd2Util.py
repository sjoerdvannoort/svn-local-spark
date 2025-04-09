from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

class Scd2Util:
    @staticmethod
    def fromSnapshots(inp, key_colnames, snapshot_colname,from_colname = "valid_from",to_colname = "valid_to"):
        inp_cols = key_colnames +[snapshot_colname]
        data_cols = [c for c in inp.columns if c not in inp_cols]

        sdw = Window.partitionBy(key_colnames).orderBy(snapshot_colname)
        return  inp.withColumn("hash",sha2(concat(*data_cols),512))\
            .withColumn("prevHash",lag("hash").over(sdw))\
            .where("hash<>prevHash or prevHash IS NULL")\
            .withColumn(to_colname, coalesce(lead(snapshot_colname).over(sdw), to_date(lit("9999-12-31"))))\
            .select(key_colnames +[col(snapshot_colname).alias(from_colname)] +[to_colname] + data_cols)