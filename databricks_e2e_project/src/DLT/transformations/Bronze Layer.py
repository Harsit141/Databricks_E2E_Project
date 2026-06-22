from pyspark import pipelines as dp
from pyspark.sql.functions import *

def create_bronze_layer(src_name):
    @dp.table(
        name=f'{src_name}'
    )
    def bronze_table():
        #read files using autoloader
        df = spark.readStream.format('cloudFiles')\
                .option('cloudFiles.format','csv')\
                .option('cloudFiles.schemaLocation', f'/Volumes/pysparkdbt/source/metadata/schema/{src_name}/')\
                .option('cloudFiles.schemaEvolutionMode','rescue')\
                .load(f'/Volumes/pysparkdbt/source/source_data/{src_name}/')
        

        #Add columns for debugging
        df = df.withColumns({
            'source_file': col('_metadata.file_name'),
            'ingestion_time': current_timestamp()
        })

        return df
    
#read source metadata
files = dbutils.fs.ls('/Volumes/pysparkdbt/source/source_data')

for f in files:
    create_bronze_layer(f.name.rstrip('/'))


