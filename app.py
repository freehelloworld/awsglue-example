import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import explode_outer
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col


args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# change to your settings

# source db is a database in glue catalog pointing to the raw data table in Redshift.
# this connecting process is done via glue crawler.
source_db = "event_db"
source_table = "raw_message"

target_db = "event_db"
target_table = "conformed_message"

# this is a connector created in glue catalog.
catalog_connector = "redshifter"


message_schema = StructType(
    [
        StructField('num_of_results', StringType()), 
        StructField('search_params', 
            StructType(
                [
                    StructField('search_distance', IntegerType()),
                    StructField('view', StringType()),
                    StructField('taxon_id', IntegerType()),
                    StructField('brand_ids', ArrayType(
                        IntegerType()
                    )),
                    StructField('published_to', StringType()),
                    StructField('store_name', StringType()),
                    StructField('root_taxon_slug', StringType())
                ]
            )
        )
    ]
)


def parse_json(array_str):
    array_str = array_str.replace("\"\"", "\"")
    json_obj = json.loads(array_str)
    return json_obj


udf_parse_json = udf(lambda msg: parse_json(msg), message_schema)

df_raw = glueContext.create_dynamic_frame.from_catalog(
    database=source_db,
    table_name=source_table,
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="raw_data_source").toDF()

df_nested = df_raw.select(df_raw.message_id, udf_parse_json(df_raw.message).alias("message"))

df_flat = df_nested.select(
    df_nested.message_id,
    col("message.num_of_results").alias('num_of_results'),
    col("message.search_params.search_distance").alias('search_distance'),
    col("message.search_params.view").alias('view'),
    col("message.search_params.taxon_id").alias("taxon_id"),
    col("message.search_params.published_to").alias("published_to"),
    col("message.search_params.root_taxon_slug").alias("root_taxon_slug"),
    col("message.search_params.store_name").alias("store_name"),
    explode_outer(df_nested.message.search_params.brand_ids).alias("brand_id")
)

df_choice = ResolveChoice.apply(
    frame=DynamicFrame.fromDF(df_flat, glueContext, 'nested'),
    choice="make_cols",
    transformation_ctx="resolvechoice2"
)

df_after_drop = DropNullFields.apply(
    frame=df_choice,
    transformation_ctx="dropnullfields3"
)

data_sink = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=df_after_drop,
    catalog_connection=catalog_connector,
    connection_options={"dbtable": target_table, "database": target_db},
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="datasink4"
)

job.commit()
