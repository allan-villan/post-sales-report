    #!/usr/bin/env python

    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    """
    The extract_vin_key_value method reads in data from data.csv to
    return a tuple containing the record's vin_number and necessary information.

    Arguments:
        Line - data read from data.csv

    Returns:
        Tuple - key: vin number
                value: tuple of (make, year, and incident type)

    """
    def extract_vin_key_value(line: str):
        values = line.split(",") # reads in data from data.csv
        incident_type = values[1]
        vin_num = values[2]
        make = values[3]
        year = values[5]
        PairRDD = (make, year, incident_type)
        return (vin_num, PairRDD)


    sc = SparkContext("local", "My Application")
    raw_rdd = sc.textFile("data.csv")
    vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

    # Only records from an Initial Sale have make and year information
    # Groups by key to populate make and year to all records using flatMap
    enhance_make = vin_kv.groupByKey()\
                        .flatMap(lambda kv: kv[1])\
                        .filter(lambda x: len(x[1]) > 0 and len(x[2]) > 0)\

    # Combine make and year for more presentable information
    make_kv = enhance_make.map(lambda x: x[0] + '-' + x[1])

    # Aggregate using reduceByKey to count each record with the same make and year
    make_kv_count = make_kv.map(lambda x: (x, 1))\
                        .reduceByKey(lambda x, y: x+y)

    # Entry point to create a dataframe and utilize SQL queries
    spark = SparkSession.builder.appName('autoinc_spark').getOrCreate()

    columns = ['make_year','count']
    df = make_kv_count.toDF(columns)

    df.sort(col("count").desc(), col("make_year").asc()).show()