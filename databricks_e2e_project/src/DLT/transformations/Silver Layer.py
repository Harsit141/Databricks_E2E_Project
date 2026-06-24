from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


################### Silver Payments ####################
@dp.temporary_view(
    name='trans_payments'
)

def trans_payments():
    df = spark.readStream\
        .table('pysparkdbt.bronze.payments')\
        .drop('_rescued_data')\
        .drop('source_file')\
        .withColumns({
            'payment_id':col('payment_id').cast(IntegerType()),
            'trip_id':col('trip_id').cast(IntegerType()),
            'customer_id':col('customer_id').cast(IntegerType()),
            'amount':col('amount').cast(DoubleType()),
            'transaction_time':col('transaction_time').cast(TimestampType()),
            'last_updated_timestamp':col('last_updated_timestamp').cast('timestamp'),
            'silver_load_timestamp':current_timestamp()
            })
    return df

@dp.table(
    name='silver_payments'
)

def silver_payments():
    return(spark.readStream.table('trans_payments'))

######################## Silver Customers #########################
@dp.temporary_view(
    name='trans_customers'
)

def trans_customers():
    df = spark.readStream.table('pysparkdbt.bronze.customers')\
        .withColumns({
            'customer_id':col('customer_id').cast(IntegerType()),
            'signup_date':col('signup_date').cast(DateType()),
            'last_updated_timestamp':col('last_updated_timestamp').cast(TimestampType()),
            'silver_load_timestamp':current_timestamp()
            })
    return df

dp.create_streaming_table("silver_customers")

dp.create_auto_cdc_flow(
    target='silver_customers',
    source='trans_customers',
    keys=['customer_id'],
    sequence_by=col('last_updated_timestamp'),
    except_column_list=['_rescued_data','source_file'],
    stored_as_scd_type=1
)


############################# Silver Drivers ################################
@dp.temporary_view(
    name='trans_drivers'
)

def trans_drivers():
    df = spark.readStream.table('pysparkdbt.bronze.drivers')\
        .withColumns({
            'driver_id':col('driver_id').cast(IntegerType()),
            'vehicle_id':col('vehicle_id').cast(IntegerType()),
            'driver_rating':col('driver_rating').cast(DoubleType()),
            'last_updated_timestamp':col('last_updated_timestamp').cast(TimestampType()),
            'silver_load_timestamp':current_timestamp()
            })
    return df

dp.create_streaming_table("silver_drivers")

dp.create_auto_cdc_flow(
    target='silver_drivers',
    source='trans_drivers',
    keys=['driver_id'],
    sequence_by=col('last_updated_timestamp'),
    except_column_list=['_rescued_data','source_file'],
    stored_as_scd_type=1
)


######################## Silver Locations ########################
@dp.temporary_view(
    name='trans_locations'
)

def trans_locations():
    df = spark.readStream.table('pysparkdbt.bronze.locations')\
        .withColumns({
            'location_id':col('location_id').cast(IntegerType()),
            'latitude':col('latitude').cast(DoubleType()),
            'longitude':col('longitude').cast(DoubleType()),
            'last_updated_timestamp':col('last_updated_timestamp').cast(TimestampType()),
            'silver_load_timestamp':current_timestamp()
            })
    return df

dp.create_streaming_table("silver_locations")

dp.create_auto_cdc_flow(
    target='silver_locations',
    source='trans_locations',
    keys=['location_id'],
    sequence_by=col('last_updated_timestamp'),
    except_column_list=['_rescued_data','source_file'],
    stored_as_scd_type=1
)

################### Silver Vehicles #######################
@dp.temporary_view(
    name='trans_vehicles'
)

def trans_vehicles():
    df = spark.readStream.table('pysparkdbt.bronze.vehicles')\
        .withColumns({
            'vehicle_id':col('vehicle_id').cast(IntegerType()),
            'year':col('year').cast(IntegerType()),
            'last_updated_timestamp':col('last_updated_timestamp').cast(TimestampType()),
            'silver_load_timestamp':current_timestamp()
            })
    return df

dp.create_streaming_table("silver_vehicles")

dp.create_auto_cdc_flow(
    target='silver_vehicles',
    source='trans_vehicles',
    keys=['vehicle_id'],
    sequence_by=col('last_updated_timestamp'),
    except_column_list=['_rescued_data','source_file'],
    stored_as_scd_type=1
)



###################### Silver Trips #####################
@dp.temporary_view(
    name='trans_trips'
)

def trans_trips():
    df = spark.readStream.table('pysparkdbt.bronze.trips')\
        .withColumns({
            'trip_id':col('trip_id').cast(IntegerType()),
            'driver_id':col('driver_id').cast(IntegerType()),
            'customer_id':col('customer_id').cast(IntegerType()),
            'vehicle_id':col('vehicle_id').cast(IntegerType()),
            'trip_start_time':col('trip_start_time').cast(TimestampType()),
            'trip_end_time':col('trip_end_time').cast(TimestampType()),
            'distance_km':col('distance_km').cast(DoubleType()),
            'fare_amount':col('fare_amount').cast(DoubleType()),
            'last_updated_timestamp':col('last_updated_timestamp').cast(TimestampType()),
            'silver_load_timestamp':current_timestamp()
            })
    return df

dp.create_streaming_table("silver_trips")

dp.create_auto_cdc_flow(
    target='silver_trips',
    source='trans_trips',
    keys=['trip_id'],
    sequence_by=col('last_updated_timestamp'),
    except_column_list=['_rescued_data','source_file'],
    stored_as_scd_type=1
)

