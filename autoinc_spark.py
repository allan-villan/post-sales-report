    #!/usr/bin/env python

    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    """
    The extract_vin_key_value method reads in data from data.csv to
    return a tuple containing the record's vin_number and necessary information.

    Arguments:
        input - data read from data.csv

    Returns:
        Tuple - key: vin number
                value: tuple of (make, year, and incident type)

    """
    def extract_vin_key_value(input: str):
        values = input.split(",") # reads in data from data.csv
        incident_type = values[1]
        vin_num = values[2]
        make = values[3]
        year = values[5]
        PairRDD = (make, year, incident_type)
        return (vin_num, PairRDD)

    # Initialize SparkContext to use Spark Cluster
    sc = SparkContext("local", "My Application")
    raw_rdd = sc.textFile("data.csv")

    # Creates key value pairs we can work with
    vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

    # Only records from an Initial Sale have make and year information
    # This line populates make and year to all records
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

    # Converts RDD to data frame so we can organize data using SQL
    columns = ['make_year','count'] # Schema for the data frame
    df = make_kv_count.toDF(columns)

    df.sort(col("count").desc(), col("make_year").asc()).show()