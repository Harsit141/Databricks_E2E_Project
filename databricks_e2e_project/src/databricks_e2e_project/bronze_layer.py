import dlt
from pyspark.sql.functions import *

def create_bronze_layer(src_name):
    @dlt.table(
        name=f'{src_name}'
    )
    def bronze_table():
        #read files using autoloader
        df = spark.readStream.format('cloudFiles')\
                .option('cloudFiles.format','csv')\
                .option('cloudFiles.schemaLocation', f'/Volumes/pysparkdbt/source/metadata/schema/{src_name}/')\
                .option('cloudFiles.schemaEvolutionMode','rescue')\
                .load(f'/Volumes/pysparkdbt/source/source_data/{src_name}/')
        
        # df = df.withColumn('source_file',col('_metadata.file_name')).withColumn('ingestion_time',current_timestamp())

        #Add columns for debugging
        df = df.withColumns({
            'source_file': col('_metadata.file_name'),
            'ingestion_time': current_timestamp()
        })

        return df
    
    # bronze_table.__name__ = f'bronze_{src_name}'
    # return bronze_table

#read source metadata
files = dbutils.fs.ls('/Volumes/pysparkdbt/source/source_data')

for f in files:
    create_bronze_layer(f.name.rstrip('/'))



    # #read files using autoloader
    # df = spark.readStream.format('cloudFiles')\
    #     .option('cloudFiles.format','csv')\
    #     .option('cloudFiles.SchemaLocation', f'/Volumes/pysparkdbt/source/metadata/schema/{src_name}/')\
    #     .option('cloudFiles.schemaEvolutionMode','rescue')\
    #     .load(f'/Volumes/pysparkdbt/source/source_data/{src_name}/')

    # #write data into tables
    # df.writeStream.format('delta')\
    #     .outputMode('append')\
    #     .trigger(once=True)\
    #     .option('checkpointLocation',f'/Volumes/pysparkdbt/source/metadata/checkpoints/{src_name}/')\
    #     .toTable(f'pysparkdbt.bronze.{src_name}')

