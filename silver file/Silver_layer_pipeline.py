import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name="stage_bookings"
)
def stage_bookings():
    df = (
        spark.readStream
        .format("delta")
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    )
    return df


@dlt.table(
    name="trans_bookings"
)
def trans_bookings():
    df = spark.readStream.table("stage_bookings")

    df = (
        df.withColumn("amount", col("amount").cast(DoubleType()))
          .withColumn("modified_date", current_timestamp())
          .withColumn("booking_date", to_date(col("booking_date"), "yyyy-MM-dd"))
          .drop("_rescued_data")
    )

    return df


rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}


@dlt.table(
    name="silver_bookings"
)
@dlt.expect_all(rules)
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df

###########################################
# flights data

@dlt.view(
    name = 'trans_flights'
)
def trans_flights():
    df = spark.readStream.format('delta')\
              .load('/Volumes/workspace/bronze/bronzevolume/flights/data/')
    df = df.drop('_rescued_data')\
            .withColumn('modifiedDate',current_timestamp())\
            .withColumn('flight_date',to_date(col('flight_date')))
    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "trans_flights",
    keys = ["flight_id"],
    sequence_by = col('insertedDate'),
    stored_as_scd_type = 1
)


#######################
#passsengers data

@dlt.view(
    name = 'trans_passengers'
)
def trans_passengers():
    df = spark.readStream.format('delta')\
            .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
    df = df.withColumn('modifiedDate',current_timestamp())\
        .drop('_rescued_data')
    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "trans_passengers",
    keys = ["passenger_id"],
    sequence_by = col('insertedDate'),
    stored_as_scd_type = 1
)


##############################
#airports_data

@dlt.view(
    name = 'trans_airports'
)
def trans_airports():
    df = spark.readStream.format('delta')\
        .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
    df = df.withColumn('modifiedDate',current_timestamp())\
        .drop('_rescued_data')
    return df


dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "trans_airports",
    keys = ["airport_id"],
    sequence_by = col('insertedDate'),
    stored_as_scd_type = 1
    )
###########################
#joined table silver_business
@dlt.table(
    name = 'silver_business'
)
def silver_business():
    
    df = dlt.readStream('silver_bookings')\
            .join(dlt.readStream('silver_passengers'),['passenger_id'])\
            .join(dlt.readStream('silver_flights'),['flight_id'])\
            .join(dlt.readStream('silver_airports'),['airport_id'])\
            .drop('modifiedDate','insertedDate','modified_date')
    return df

#notice here we are just gonna see modified_date for bookings as it is different all has modifiedDate it has modified_date
    