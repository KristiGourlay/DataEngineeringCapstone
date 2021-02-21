import pandas as pd
import configparser
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, udf, col, create_map, lit, regexp_replace
import pyspark.sql.functions as F
import re 
from itertools import chain


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['DEFAULT']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['DEFAULT']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    
    """Creates a Spark session"""
    
    spark = SparkSession.builder.\
    config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11",)\
    .enableHiveSupport() \
    .getOrCreate()
    
    return spark


def port_city_mapping(label_descriptions):
    
    """Creates a mapping dictionary to get the city name for dimension tables"""
    
    with open(label_descriptions) as f:
        lines = f.readlines()

    re_compiled = re.compile(r"\'(.*)\'.*\'(.*)\'")

    port_names = {}

    for l in lines[302:961]:
        results = re_compiled.search(l)
        port_names[results.group(1)] = results.group(2)
        
    mapping_expr = create_map([lit(x) for x in chain(*port_names.items())])
    
    return mapping_expr


def quality_checks(data_table_name):

    """Performs two data checks on a table when called"""
    
    if data_table_name is not None:
        if data_table_name.count() != 0:
            print('Table exists and contains records')
        else:
            raise ValueError('Data Quality Check Failed: table exists, but does not contain records')
    else:
        raise ValueError('Data Quality Check Failed: table does not exists')


        
def process_temperature_data(spark, temperature_input_data, output_data):
    
    """Processes temperature data; creating a table with the average monthly temperature for each city since 1990"""
    
    temperature_staging_table = spark.read.csv(temperature_input_data, header='true')
    
    temperature_staging_table = temperature_staging_table.dropna(subset=['AverageTemperature'])
    temperature_staging_table = temperature_staging_table.filter((col("dt") >= "1990-01-01") & (col("Country") == 'United States'))

    temperature_staging_table = temperature_staging_table.select(F.year('dt').alias('year'),
                                       F.month('dt').alias('month'),
                                       F.col('AverageTemperature').alias('average_temperature'), 
                                       F.col('City').alias('city'), 
                                       F.col('Country').alias('country'), 
                                       F.col('Latitude').alias('latitude'), 
                                       F.col('Longitude').alias('longitude')).dropDuplicates() \
                                       .groupby(['city', 'country', 'latitude', 'longitude', 'month']).agg(F.mean('average_temperature'))

    temperature_staging_table = temperature_staging_table.withColumnRenamed("avg(average_temperature)", "average_temperature")

    temperature_staging_table = temperature_staging_table.withColumn('temperature_id', F.monotonically_increasing_id())
    
    quality_checks(temperature_staging_table)
    temperature_staging_table.write.parquet(os.path.join(output_data, 'temperature_table.parquet'), mode='overwrite', partitionBy=['country', 'city'])

    print('Temperature Dimension Table is completed')


def process_demographics_data(spark, demographics_input_data, output_data):
    
    """Processes, cleans, and formats demographics data into Demographics dimension table"""
    
    df_demographics_og = spark.read.csv(demographics_input_data, header='true', sep=';')

    df_demographics = df_demographics_og.select(F.col('City').alias('city_name'),
                                            F.col('State').alias('state_name'),
                                            F.col('Median Age').alias('median_age'),
                                            F.col('Male Population').alias('male_population'),
                                            F.col('Female Population').alias('female_population'),
                                            F.col('Total Population').alias('total_population'),
                                            F.col('Number of Veterans').alias('veteran_population'),
                                            F.col('Foreign-born').alias('foreign_born_population'),
                                            F.col('Average Household Size').alias('average_household_size'),
                                            F.col('State Code').alias('state_code')).dropDuplicates()

    df_demographics = df_demographics.withColumn('city_id', 
        F.concat(F.col('city_name'),F.lit('_'), F.col('state_name')))
    
    
    df_race_demographics = df_demographics_og.withColumn("Count",col("Count").cast('int'))


    clean_col = udf(lambda x: x.replace(' ', '_'))
    df_race_demographics = df_race_demographics.withColumn('Race', clean_col(df_race_demographics.Race))

    df_race_demographics = df_race_demographics.select(F.col('City').alias('city_name'),
                                            F.col('State').alias('state_name'),                                
                                            F.col('State Code').alias('state_code'),
                                            F.col('Race').alias('race'),
                                            F.col('Count').alias('count')).dropDuplicates() \
                                            .groupBy('city_name', 'state_name', 'state_code') \
                                            .pivot('race').sum('count')

    df_race_demographics = df_race_demographics.withColumn('city_id', 
        F.concat(F.col('city_name'),F.lit('_'), F.col('state_name')))
    
 
   
    df_demographics.createOrReplaceTempView("df_demographics_table")
    df_race_demographics.createOrReplaceTempView("df_race_demographics_table")
    
    sqlContext = SQLContext(spark)

    df_total_demographics = sqlContext.sql("Select tb2.*, tb1.median_age, tb1.male_population, tb1.female_population, tb1.total_population, tb1.veteran_population, tb1.foreign_born_population,tb1.average_household_size FROM df_demographics_table tb1 JOIN df_race_demographics_table tb2 ON tb1.city_id = tb2.city_id")   

    mapping = dict(zip(['American_Indian_and_Alaska_Native', 'Asian', 'Black_or_African-American', 'Hispanic_or_Latino', 'White'], ['american_indian_and_alaskan_native_population', 'asian_population', 'black_or_african_american_population', 'hispanic_or_latino_population']))
    
    for k, v in mapping.items():
        demographics_staging_table = df_total_demographics.withColumnRenamed(k, v)

    quality_checks(demographics_staging_table)

    demographics_staging_table.write.parquet(os.path.join(output_data, 'demographics_table.parquet'), mode='overwrite', partitionBy=['state_name', 'city_name'])

    print('Demographics Dimension table is completed')
    
    
def process_airport_data(spark, airport_input_data, mapping_dict, output_data):
    
    """ Processes airport data and create Airport Dimension Table """
    
    airport_staging_table = spark.read.csv(airport_input_data, header='true')
    
    get_lat = udf(lambda x: x.split(', ')[0])
    get_long = udf(lambda x: x.split(', ')[1])
    get_country_code = udf(lambda x: x.split('-')[0])
    get_state_code = udf(lambda x: x.split('-')[1])


    airport_staging_table = airport_staging_table.withColumn('latitude', get_lat(airport_staging_table.coordinates))
    airport_staging_table = airport_staging_table.withColumn('longitude', get_long(airport_staging_table.coordinates))
    airport_staging_table = airport_staging_table.withColumn('country_code', get_country_code(airport_staging_table.iso_region))
    airport_staging_table = airport_staging_table.withColumn('state_code', get_state_code(airport_staging_table.iso_region))


    airport_staging_table = airport_staging_table.withColumn("city", mapping_dict.getItem(col("iata_code")))
    
    airport_staging_table = airport_staging_table.select(F.col('ident').alias('airport_id'),        
                                          'name',
                                          'city',
                                          'country_code',
                                          'state_code',
                                          'iata_code',
                                          'latitude',
                                          'longitude').dropDuplicates()
    
    quality_checks(airport_staging_table)
    
    airport_staging_table.write.parquet(os.path.join(output_data, 'airport_code_table.parquet'), mode='overwrite')

    print('Airport Dimension Table is completed')
    
    
def process_immigration_data(spark, immigration_input_data, mapping_dict, output_data):
    
    """ Processes immigration data and creates Immigration dimension table and Fact Table """
    
    immigration_staging_table = spark.read.format('com.github.saurfang.sas.spark').load(immigration_input_data)

    get_datetime = udf(lambda x: datetime(1960, 1, 1) + timedelta(days=int(x)))
    immigration_staging_table = immigration_staging_table.withColumn('arrival_date', get_datetime(immigration_staging_table.arrdate))

    immigration_staging_table = immigration_staging_table.withColumn("city", mapping_dict.getItem(col("i94port")))

    immigration_dimension_table = immigration_staging_table.select(
                    F.col('cicid').alias('immigration_number'),    
                    F.col('i94bir').alias('age'),
                    F.col('i94visa').alias('visa_type'),
                    F.col('biryear').alias('birth_year'),
                    'gender'
                    ).dropDuplicates()
    
    quality_checks(immigration_dimension_table)
    immigration_staging_table.write.parquet(os.path.join(output_data, 'immigration_table.parquet'), mode='overwrite')

    print('Immigration Dimesion Table is completed')
    
    immigration_fact_table = immigration_staging_table.select(
                    F.col('cicid').alias('immigration_number'),
                    'city',
                    F.col('i94addr').alias('state_code'),
                    F.col('i94port').alias('port'),
                    F.col('i94mon').alias('month'),
                    F.col('i94yr').alias('year')
                    ).drop_duplicates()

    quality_checks(immigration_fact_table)
    immigration_fact_table.write.parquet(os.path.join(output_data, 'immigration_fact_table.parquet'), mode='overwrite')

    print('Immigration Fact Table is completed')
    
    
def main():
    
    """Uses function defined above to load data and create parquet files"""

    temperature_input_data = '../../data2/GlobalLandTemperaturesByCity.csv'
    demographics_input_data = 'input-data/us-cities-demographics.csv'
    airport_input_data = 'input-data/airport-codes_csv.csv'
    immigration_input_data = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    label_descriptions = 'input-data/I94_SAS_Labels_Descriptions.SAS'
    output_data = 'output-data'
    
    
    
    spark = create_spark_session()
    
    mapping_dict = port_city_mapping(label_descriptions)
    
    process_demographics_data(spark, demographics_input_data, output_data)
    
    process_immigration_data(spark, immigration_input_data, mapping_dict, output_data)
                                   
    process_airport_data(spark, airport_input_data, mapping_dict, output_data)

    process_temperature_data(spark, temperature_input_data, output_data) 


    
if __name__ == "__main__":
    main()
