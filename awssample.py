import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # You can set it to DEBUG, INFO, WARN, etc.

print("Started")
logger.info(f"P R O C E S S   S T A R T E D ")
start_time = time.time()
import psutil
import os

def log_memory_usage():
    process = psutil.Process(os.getpid())
    memory_used = process.memory_info().rss / (1024 ** 2)  # Convert bytes to MB
    logger.info(f"Memory Usage: {memory_used:.2f} MB")
    print(f"Memory Usage: {memory_used:.2f} MB")


log_memory_usage()  # Log at the start
def read_json(glue_context: GlueContext, path) -> DynamicFrame:
    dynamicframe = glue_context.create_dynamic_frame.from_options(
        connection_type='s3',
        connection_options={
            'paths': [path],
            'recurse': True
        },
        format='json'
    )
    return dynamicframe


params = []
if '--JOB_NAME' in sys.argv:
    params.append('JOB_NAME')
args = getResolvedOptions(sys.argv, params)

context = GlueContext(SparkContext.getOrCreate())
job = Job(context)

if 'JOB_NAME' in args:
    jobname = args['JOB_NAME']
else:
    jobname = "test"
job.init(jobname, args)

# Datasource from public AWS S3 Bucket available in https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-samples-legislators.html
dyf = read_json(context, "s3://awsglue-datasets/examples/us-legislators/all/persons.json")
# Same file, from local filestore. Uncomment and comment previous
# dyf = read_json(context, "persons.json")
dyf.printSchema()
print("\n"*10) # Separator to ease logging 
print("Glue Dynamic Frame preview")
dyf.show()
logger.info("Mapping family_name to last_name")
mapped_dyf = dyf.apply_mapping([
    ("family_name", "String", "last_name", "String"),
    ('name', 'string', 'name', 'string'),
    ('gender', 'string', 'gender', 'string'),
    ('image', 'string', 'image', 'string'),
    ('sort_name', 'string', 'sort_name', 'string'),
    ('given_name', 'string', 'given_name', 'string'),
    ('birth_date', 'string', 'birth_date', 'string'),
    ('id', 'string', 'id', 'string'),
    ('death_date', 'string', 'death_date', 'string')
    ])
print("\n\nSchema with fields renamed: ")
mapped_dyf.printSchema()

# Convert to DataFrame
spark_df = mapped_dyf.toDF()
print("Spark Data Frame preview")
spark_df.show()
logger.info("Writing parquet file")
log_memory_usage()  # Log at the start

mapped_dyf.write(
    connection_type="file",
    connection_options={
        "path": "persons_parsed.parquet",
        "partitionKeys": []  # Ensures overwriting
        },
    format="parquet" 
    )
# print("writing parquet to s3")

# mapped_dyf.write(
#     connection_type="s3",
#     connection_options={
#         "path": "s3://aws-glue-temporary-515951668509-us-east-1/glue/persons_parsed.parquet",
#         "partitionKeys": []  # Ensures overwriting
#         },
#     format="parquet" 
#     )

# n_df = mapped_dyf.toDF()
# n_df.write.parquet("persons.parquet", mode="overwrite")

# glueContext.write_dynamic_frame.from_options(
#     dynamic_frame,
#     connection_type="s3",
#     connection_options={"path": output_dir},
#     format="parquet"
# )

# def rename_columns(dynamic_frame: DynamicFrame, column_mapping) ->DynamicFrame:
#     """
#     Rename or remap columns in a DynamicFrame.

#     Args:
#         dynamic_frame: The input DynamicFrame.
#         column_mapping: A dictionary or list of tuples specifying mappings.
#                         Format:
#                           - Dict: {"old_col": ("new_col", "new_type"), ...}
#                           - List: [("old_col", "new_col", "new_type"), ...]

#     Returns:
#         A new DynamicFrame with updated column names/types.
#     """
#     mapping = []
    
#     # Handle different input formats
#     if isinstance(column_mapping, dict):
#         column_mapping = [
#             (old_col, new_col, new_type)
#             for old_col, (new_col, new_type) in column_mapping.items()
#         ]
    
#     # Build the mapping for apply_mapping
#     for column in dynamic_frame.toDF().schema.fields:
#         source_name = column.name
#         source_type = column.dataType.typeName()
        
#         # Default to original column name and type
#         target_name = source_name
#         target_type = source_type
        
#         # Check if the column needs to be renamed or retyped
#         for old_col, new_col, new_type in column_mapping:
#             if source_name == old_col:
#                 target_name = new_col
#                 target_type = new_type if new_type else source_type
#                 break
        
#         mapping.append((source_name, source_type, target_name, target_type))
    
#     # Apply the mapping
#     return dynamic_frame.apply_mapping(mapping)

# mapped_dyf_renamed = rename_columns(dyf, {"gender": ("sex", "string")})
# mapped_dyf_renamed.printSchema()
# mapped_dyf_renamed.show()
print(("*"*20+"\n")*10)

# def change_sex_with_single_char(rec):
#     if str(rec["gender"]).lower() == "male":
#         rec["new_gender"] = "M"
#     elif str(rec["gender"]).lower() == "female":
#         rec["new_gender"] = "F"
#     else:
#         rec["new_gender"] = "x"

#     return rec

# mapped_dyF =  dyf.map(f = change_sex_with_single_char)

# mapped_dyF.printSchema()
# mapped_dyF.show()

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Finished in {elapsed_time} seconds")
