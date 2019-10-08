#Import libraries
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql.types import LongType
from pyspark.sql import types
from pyspark.sql.functions import trim
from pyspark.sql.types import *

#Create a connection to PostgreSql database
url_name="url_name"
table_name="table"
mode="overwrite"
user_name="user"
pasword="password"
driver="org.postgresql.Driver"

# create a spark session
def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("usfsproj") \
        .getOrCreate()
    return spark

# Functins to read data from S3 and to create dataframes
def process_plot_data(spark, input, output):
    # create plot data frame
    plot = spark.read.format("avro").load("s3a://bucketname")
    #print(plot.dtypes)
    plot_df = plot.select(['plot_sequence_number',
                            'survey_sequence_number',
                            'county_sequence_number',
                            'plot_inventory_year',
                            'plot_state_code',
                            'plot_state_code_name',
                            'plot_survey_unit_code',
                            'plot_county_code',
                            'plot_phase_2_plot_number',
                            'plot_status_code',
                            'measurement_year',
                            'sample_kind_code',
                            'sample_kind_code_name',
                            'plot_design_code',
                            'latitude',
                            'longitude',
                            'elevation',
                            'p2_vegetation_sampling_status_code',
                            'p2_vegetation_sampling_status_code_name',
                            'p2_vegetation_sampling_level_detail_code',
                            'p2_vegetation_sampling_level_detail_code_name',
                            'unique_plot',
                            'precipitation'])
    # filters data by state
    plot_df = plot_df.filter(plot_df['plot_state_code_name']=='California')

    # write data to PostgreSql table
    plot_df.write \
       .format("jdbc") \
       .option("url", url_name) \
       .option("dbtable", "table") \
       .option("user", user) \
       .option("password", pasword) \
       .option("driver", "org.postgresql.Driver") \
       .mode("overwrite").save()

def process_plot_condition_data(spark, input_data, output_data):
    #Creatse dataframe\ by joining multiple
    plot_df = spark.read.format("avro").load("s3a://bucketname")
    condition_df = spark.read.format("avro").load("s3a://bucketnmae")
    #print(plot_df.dtypes)

    # joins dataframes
    plot_condition_df = plot_df.join(condition_df,(plot_df.plot_sequence_number== condition_df.plot_sequence_number))\
                .select(plot_df.plot_sequence_number, plot_df.plot_inventory_year,
                plot_df.plot_state_code, plot_df.plot_state_code_name,
                plot_df.plot_survey_unit_code, plot_df.plot_county_code,
                plot_df.intensity, plot_df.plot_status_code,
                plot_df.plot_status_code_name,
                plot_df.plot_design_code, plot_df.latitude,
                plot_df.longitude, plot_df.elevation,
                plot_df.p2_vegetation_sampling_status_code,
                plot_df.p2_vegetation_sampling_status_code_name,
                condition_df.inventory_year, condition_df.state_code_name,
                condition_df.county_code, condition_df.county_code_name,
                condition_df.forest_type_code, condition_df.forest_type_code_name,
                condition_df.field_forest_type_code, condition_df.field_forest_type_code_name,
                condition_df.mapping_density, condition_df.mapping_density_name,
                condition_df.stand_age, condition_df.condition_proportion_unadjusted,
                condition_df.microplot_proportion_unadjusted,
                condition_df.subplot_proportion_unadjusted,
                condition_df.macroplot_proportion_unadjusted,
                condition_df.slope, condition_df.aspect,
                condition_df.basal_area_per_acre_of_live_trees,
                condition_df.fieldrecorded_stand_age,
                condition_df.alllivetree_stocking_percent,
                condition_df.growingstock_stocking_percent
                )
    # filters by state
    plot_condition_df = plot_condition_df.filter(plot_condition_df['plot_state_code_name']=='California')
    # plot_condition_df.show(10)

    #Write data to PostgresSql table
    plot_condition_df.write \
        .format("jdbc") \
        .option("url", url_name) \
        .option("dbtable", "table") \
        .option("user", user) \
        .option("password", pasword) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite").save()
    #print(plot_condition_df.take(1))
def process_fire_data(spark, input_data, output_data):

    #read a csv file from S3 and create a data frame
    fires = spark.read.csv("s3a:csv file", header=True, inferSchema=True)
    fires = fires.withColumn("SOURCE_REPORTING_UNIT_NAME", trim(fires.SOURCE_REPORTING_UNIT_NAME))

    #fires.dtypes
    #fires.printSchema()
    fires_df = fires.select(['OBJECTID', #'FOD_ID', #'FPA_ID',
                'SOURCE_SYSTEM_TYPE', 'SOURCE_SYSTEM',
                'NWCG_REPORTING_UNIT_NAME',
                'SOURCE_REPORTING_UNIT',
                'OURCE_REPORTING_UNIT_NAME',
                'LOCAL_FIRE_REPORT_ID',
                'FIRE_CODE', 'FIRE_NAME',
                'FIRE_YEAR', #'DISCOVERY_DATE',
                #'CONT_DATE', 'FIRE_SIZE',
                'FIRE_SIZE_CLASS',
                'LATITUDE', 'LONGITUDE',
                'OWNER_CODE','OWNER_DESCR',
                'STATE', 'COUNTY'])

    # creates a new column
    fires_df = fires_df.filter(fires_df['STATE']=='CA')
    fires = fires.withColumn("STATE_NAME",
    F.when((col("STATE")=="CA"), "California"))
    fires_df.dropna

    #Write data to PostgreSql table
    fires_df.write \
      .format("jdbc") \
      .option("url", url_name) \
      .option("dbtable", table) \
      .option("user", user) \
      .option("password", pasword) \
      .option("driver", "org.postgresql.Driver") \
      .mode("overwrite").save()

spark.stop()

def main():
    spark = create_spark_session()
    input_data = "s3a://"
    output_data = ""

    process_plot_data(spark, input_data, output_data)
    process_fire_data(spark, input_data, output_data)
    #process_stations_data(spark, input_data, output_data)
    process_plot_condition_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
