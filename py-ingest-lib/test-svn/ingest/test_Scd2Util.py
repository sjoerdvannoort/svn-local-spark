import unittest
from pyspark.sql import SparkSession
from svn.ingest.Scd2Util import Scd2Util
import json
from datetime import date


def json_serial(obj):
    if isinstance(obj, (date)):
        return obj.isoformat()
    raise TypeError("Type not serializable")

class Test_Scd2Util(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession \
            .builder \
            .appName("SvnLocalSpark") \
            .master("local")\
            .getOrCreate()
        #print(f"spark {self.spark.version} {self.spark.sparkContext.uiWebUrl}")
        return super().setUp()

    def test_fromSnapshots1(self):
        src = self.spark.read.options(delimiter=",", header=True, inferSchema=True).csv("resources/sourcedata/commercial_property_snapshots_100_M39.csv")
        src_test1 = src.where("property_id='P012' and `date`<date '2023-01-01'")
        result_df =   Scd2Util.fromSnapshots(src_test1,["property_id"],"date")
        #result_df.show()

        result_rws = result_df.select("valid_from", "valid_to", "property_value", "energy_label").collect()
        result_json = json.dumps(result_rws,default=json_serial)

        expected_rws = """[
            ["2022-01-01", "2022-01-05", 187462.36, "C"], 
            ["2022-01-05", "2022-01-18", 191214.34, "C"], 
            ["2022-01-18", "2022-02-19", 196199.71, "C"], 
            ["2022-02-19", "2022-03-13", 200512.03, "C"], 
            ["2022-03-13", "2022-03-29", 199764.19, "C"], 
            ["2022-03-29", "2022-04-26", 205549.8, "C"], 
            ["2022-04-26", "2022-04-29", 202322.08, "C"], 
            ["2022-04-29", "2022-06-02", 199776.09, "C"], 
            ["2022-06-02", "2022-06-07", 200637.3, "C"], 
            ["2022-06-07", "2022-06-19", 201827.14, "C"], 
            ["2022-06-19", "2022-07-02", 202877.08, "C"], 
            ["2022-07-02", "2022-08-05", 213327.15, "C"], 
            ["2022-08-05", "2022-09-14", 225297.82, "C"], 
            ["2022-09-14", "2022-12-04", 225297.82, "E"], 
            ["2022-12-04", "2022-12-09", 237590.49, "E"], 
            ["2022-12-09", "9999-12-31", 251398.39, "E"]
        ]"""
        # serialize and deserialize literal to remove whitespace
        expected_json = json.dumps(json.loads(expected_rws))

        self.assertEqual(expected_json, result_json)


if __name__ == '__main__':
    # this is the entry point if you select this file and select "Run Python File"
    # it is NOT called from the Testing extension
    unittest.main()