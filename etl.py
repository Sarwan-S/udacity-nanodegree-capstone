#library imports
import configparser
from datetime import datetime
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as func


#read config file
config = configparser.ConfigParser()
config.read('dl.cfg')


#set aws credentials into os environment variables
os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Description: Function to create a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def cleanse_liquor_sales_data(spark, input_data):
    """
    Description: Function to read and cleanse staging liquor sales data
    Inputs: 
        spark: spark session object
        input_data: location of staging data
    """
    
    #define schema for liquor_sales data
    liquor_sales_schema = StructType(
        [
            StructField('invoice_number', StringType(), False),
            StructField('sales_date', DateType(), True),
            StructField('store_number', DoubleType(), True),
            StructField('store_name', StringType(), True),
            StructField('address', StringType(), True),
            StructField('city', StringType(), True),
            StructField('zipcode', IntegerType(), True),
            StructField('store_location', StringType(), True),
            StructField('county_number', DoubleType(), True),
            StructField('county', StringType(), True),
            StructField('category', DoubleType(), True),
            StructField('category_name', StringType(), True),
            StructField('vendor_number', DoubleType(), True),
            StructField('vendor_name', StringType(), True),
            StructField('item_number', DoubleType(), True),
            StructField('description', StringType(), True),
            StructField('pack', IntegerType(), True),
            StructField('bottle_volume', IntegerType(), True),
            StructField('item_cost_price', DecimalType(10,2), True),
            StructField('item_retail_price', DecimalType(10,2), True),
            StructField('bottles_sold', IntegerType(), True),
            StructField('sales_usd', DecimalType(10,2), True),
            StructField('volume_sold_litres', DecimalType(10,2), True),
            StructField('volume_sold_gallons', DecimalType(10,2), True)
        ]
    )
    
    #read data in; data file should not have headers
    try:
        print("reading liquor sales staging data")
        df_spark = spark\
                .read\
                .option("header","false")\
                .schema(liquor_sales_schema)\
                .option("dateFormat", "d/M/y")\
                .csv(input_data)
    except Exception as e:
        print("Error reading liquor_sales staging data: " + str(e))
    
    
    #split Store Location into latitude and longitude
    try:
        print("splitting store_location into latitude and longitude")
        split_col = func.split(df_spark['store_location'], " ")
        df_spark = df_spark.withColumn('latitude', func.regexp_replace(split_col.getItem(2), "\)", ''))
        df_spark = df_spark.withColumn('longitude', func.regexp_replace(split_col.getItem(1), "\(", ''))
    except Exception as e:
        print("Error splitting store location field into latitude and longitude: " + str(e))
    
    
    #fix values in categorical name field
    try:
        print("fixing values in category_name field")
        df_spark = df_spark.withColumn('category_name', 
                                       func.when(df_spark['category_name'] == "Cocktails /RTD" , "Cocktails / RTD")
                                       .when(df_spark['category_name'] == "American Cordials & Liqueur" , "American Cordials & Liqueurs")
                                       .when(df_spark['category_name'] == "American Vodkas" , "American Vodka")
                                       .when(df_spark['category_name'] == "Imported Cordials & Liqueur" , "Imported Cordials & Liqueurs")
                                       .when(df_spark['category_name'] == "Imported Distilled Spirits Specialty" , "Imported Distilled Spirit Specialty")
                                       .when(df_spark['category_name'] == "Imported Vodkas" , "Imported Vodka")
                                       .when(df_spark['category_name'] == "Temporary &  Specialty Packages" , "Temporary & Specialty Packages")
                                       .otherwise(df_spark['category_name']))
    except Exception as e:
        print("Error fixing values in category_name field: "+ str(e))

    
    #convert county into proper case
    try:
        print("converting county field into proper case")
        df_spark = df_spark.withColumn('county', func.initcap('county'))
    except Exception as e:
        print("Error converting county field into proper case: " + str(e))
    
    
    print("cleansing of liquor_sales data complete")
    df_spark.show(2)
    return df_spark



def cleanse_holidays_data(spark, input_data):
    """
    Description: Function to read and cleanse staging holiday data
    Inputs: 
        spark: spark session object
        input_data: location of staging data
    """
    
    #read data in
    try:
        print("reading holidays staging data")
        df_holidays_spark = spark.read\
                            .option("multiline","true")\
                            .json(input_data)
    except Exception as e:
        print("Error reading holidays staging data: " + str(e))
    
    
    #cast Date field as date format
    try:
        print("casting date field into date format")
        df_holidays_spark = df_holidays_spark.withColumn("Date", func.to_date("Date", "dd/MM/yy"))
    except Exception as e:
        print("Error casting date field into date format")
    
    
    #rename columns to lowercase
    try:
        print("renaming columns to lowercase")
        df_holidays_spark = df_holidays_spark.withColumnRenamed('Date','date')
        df_holidays_spark = df_holidays_spark.withColumnRenamed('Holiday','holiday_name')
    except Exception as e:
        print("Error renaming columns to lowercase:" + str(e))
    
    
    print("cleansing of holidays data complete")
    df_holidays_spark.show(2)
    return df_holidays_spark



def cleanse_weather_data(spark, input_data):
    """
    Description: Function to read and cleanse staging weather data
    Inputs: 
        spark: spark session object
        input_data: location of staging data
    """
    
    #read data in
    try:
        print("reading weather staging data")
        df_weather_spark = spark.read\
                            .option("header","true")\
                            .option("inferSchema", "true")\
                            .csv(input_data)
    except Exception as e:
        print("Error reading weather staging data: " + str(e))
    
    
    #rename columns
    try:
        print("renaming columns")
        df_weather_spark = df_weather_spark.withColumnRenamed('County','county')
        df_weather_spark = df_weather_spark.withColumnRenamed('State','state')
        df_weather_spark = df_weather_spark.withColumnRenamed('Average Temperature','climate_temp')
        df_weather_spark = df_weather_spark.withColumnRenamed('Latitude (generated)','latitude_generated')
        df_weather_spark = df_weather_spark.withColumnRenamed('Longitude (generated)','longitude_generated')
        df_weather_spark = df_weather_spark.withColumnRenamed('Year','year')
        df_weather_spark = df_weather_spark.withColumnRenamed('Month','month')
    except Exception as e:
        print("Error renaming columns: " + str(e))
    
    
    print("cleansing of weather data complete")
    df_weather_spark.show(2)
    return df_weather_spark



def process_data(spark, liquor_sales_data, holidays_data, weather_data, output_data):
    """
    Description: Function to transform cleansed data into fact and dimension tables
    Inputs: 
        spark: spark session object
        liquor_sales_data: cleansed liquor_sales dataframe
        holidays_data: cleansed holidays dataframe
        weather_data: cleansed weather dataframe
        output_data: location to save the final tables to
    """
    
    liquor_sales_data.createOrReplaceTempView("liquor_sales_data")
    holidays_data.createOrReplaceTempView("holidays_data")
    weather_data.createOrReplaceTempView("weather_data")

    
    #items dimension table
    try:  
        print("processing items table")
        items = spark.sql("""
            SELECT DISTINCT
                    item_number,
                    description,
                    category_name,
                    bottle_volume,
                    pack
            FROM liquor_sales_data
        """)

        print("items")
        items.show(2)
    except Exception as e:
        print("Error processing into items table: " + str(e))

    
    #vendors dimension table
    try:
        print("processing vendors table")
        vendors = spark.sql("""
            SELECT DISTINCT
                    vendor_number,
                    vendor_name
            FROM liquor_sales_data
        """)

        print("vendors")
        vendors.show(2)
    except Exception as e:
        print("Error processing into vendors table: " + str(e))

    
    #counties dimension table
    try:
        print("processing counties table")
        counties = spark.sql("""
            SELECT DISTINCT
                    county_number,
                    county
            FROM liquor_sales_data
        """)

        print("counties")
        counties.show(2)
    except Exception as e:
        print("Error processing into counties table")

    
    #stores dimension table
    try:
        print("processing stores table")
        stores = spark.sql("""
            SELECT DISTINCT
                    store_number,
                    store_name,
                    address,
                    city,
                    zipcode,
                    latitude,
                    longitude
            FROM liquor_sales_data
        """)

        print("stores")
        stores.show(2)
    except Exception as e:
        print("Error processing into stores table")

    
    #time dimension table
    try:
        print("processing time table")
        time = spark.sql("""
            SELECT DISTINCT
                    sales.sales_date,
                    day(sales.sales_date) as day,
                    weekofyear(sales.sales_date) as week,
                    month(sales.sales_date) as month,
                    year(sales.sales_date) as year,
                    dayofweek(sales.sales_date) as weekday,
                    case
                        when holidays.holiday_name is null then False
                        else True
                    end as is_holiday,
                    holidays.holiday_name
            FROM liquor_sales_data sales
            LEFT JOIN holidays_data holidays
            ON sales.sales_date = holidays.date
        """)

        print("time")
        time.show(2)
    except Exception as e:
        print("Error processing into time table")

    
    #liquor_sales fact table
    try:
        print("processing liquor_sales table")
        liquor_sales = spark.sql("""
            SELECT
                    sales.invoice_number,
                    sales.sales_date,
                    sales.store_number,
                    sales.county_number,
                    sales.item_number,
                    sales.vendor_number,
                    sales.bottles_sold,
                    sales.volume_sold_litres,
                    sales.item_cost_price,
                    sales.item_retail_price,
                    sales.sales_usd,
                    weather.climate_temp
            FROM liquor_sales_data sales
            LEFT JOIN weather_data weather
            ON year(sales.sales_date) = weather.year
            AND month(sales.sales_date) = weather.month
            AND sales.county = weather.county
        """)

        print("liquor_sales")
        liquor_sales.show(2)
    except Exception as e:
        print("Error processing liquor_sales table")
    
    
    #data quality checks on counts
    print("performing data quality checks on counts")
    list_of_errors = []

    if items.count() < 1:
        list_of_errors.append("items table count has no records")
    if vendors.count() < 1:
        list_of_errors.append("vendors table count has no records")
    if counties.count() < 1:
        list_of_errors.append("counties table count has no records")
    if stores.count() < 1:
        list_of_errors.append("stores table count has no records")
    if time.count() < 1:
        list_of_errors.append("time table count has no records")
    if liquor_sales.count() < 1:
        list_of_errors.append("liquor_sales table count has no records")

    if len(list_of_errors) >= 1:
        for error in list_of_errors:
            print(error)
    else:
        print("No issues with count checks")
        
    
    #data quality checks on null values
    list_of_errors = []

    if liquor_sales.where("invoice_number is null").count() > 0:
        list_of_errors.append("null records detected in invoice_number field of liquor_sales table")

    if len(list_of_errors) >= 1:
        for error in list_of_errors:
            print(error)
    else:
        print("No issues with null checks")
    
    
    #save tables as parquet files
    try:
        print("saving items table as parquet")
        items.repartition(2).write.parquet(output_data + "/items")
    except Exception as e:
        print("Error saving items table to parquet file: " + str(e))
   
    try:
        print("saving vendors table as parquet")
        vendors.repartition(2).write.parquet(output_data + "/vendors")
    except Exception as e:
        print("Error saving vendors table to parquet file: " + str(e))
    
    try:
        print("saving counties table as parquet")
        counties.repartition(1).write.parquet(output_data + "/counties")
    except Exception as e:
        print("Error saving counties table to parquet file: " + str(e))
    
    try:
        print("saving stores table as parquet")
        stores.repartition(2).write.parquet(output_data + "/stores")
    except Exception as e:
        print("Error saving stores table to parquet file: " + str(e))
    
    try:
        print("saving time table as parquet")
        time.repartition(1).write.parquet(output_data + "/time")
    except Exception as e:
        print("Error saving time table to parquet file: " + str(e))
    
    try:
        print("saving liquor_sales table as parquet")
        liquor_sales.repartition(5).write.parquet(output_data + "/liquor_sales")
    except Exception as e:
        print("Error saving liquor_sales table to parquet file: " + str(e))
        
    
    print("processing complete")



def main():
    """
    Description: main function for running respective functions
    """
    spark = create_spark_session()
    
    
    liquor_sales_staging = "s3a://sarwan-capstone/staging/liquor_sales/Iowa_Liquor_Sales_noheaders.csv"
    holidays_staging = "s3a://sarwan-capstone/staging/holidays/usholidays.json"
    weather_staging = "s3a://sarwan-capstone/staging/weather"
    
    #if running using local data samples, use the following instead
    #liquor_sales_staging = "file:///home/workspace/data/liquor_sales/Iowa_Liquor_Sales_sample_noheaders.csv"
    #holidays_staging = "file:///home/workspace/data/holidays/usholidays.json"
    #weather_staging = "file:///home/workspace/data/weather"
    
    
    output_data = "" #e.g "s3a://my-s3-bucketname"
    
    
    liquor_sales_cleansed = cleanse_liquor_sales_data(spark, liquor_sales_staging)
    holidays_cleansed = cleanse_holidays_data(spark, holidays_staging)
    weather_cleansed = cleanse_weather_data(spark, weather_staging)
    process_data(spark, liquor_sales_cleansed, holidays_cleansed, weather_cleansed, output_data)
    
    
    spark.stop()



if __name__ == "__main__":
    main()