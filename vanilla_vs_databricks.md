# Databricks features not in standard spark

## EXCEPT (in select list)
```
SELECT  * EXCEPT(colb,colc) FROM myTable
```

## CREATE TABLE defaults to delta
In standard spark, default create table statement defaults to a hive table, even if hive support is not enabled.
```
CREATE TABLE MyTable
(ColA string
ColB string)
USING delta
```

## COPY INTO
```
COPY INTO default.loan_risks_upload
FROM '/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet'
FILEFORMAT = PARQUET;
```

## read_files table valued function
```
SELECT * FROM read_files(
    's3://bucket/path',
    format => 'csv',
    inferSchema => true,
    header => true)
```