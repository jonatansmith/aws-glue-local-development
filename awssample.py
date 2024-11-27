import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import time

print("Started")
start_time = time.time()

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

# dyf = read_json(context, "s3://awsglue-datasets/examples/us-legislators/all/persons.json")
dyf = read_json(context, "persons.json")
dyf.printSchema()
# show df
print("\n"*10)
dyf.show()

# mapped_dyf = dyf.apply_mapping([("family_name", "String", "last_name", "String")])
# mapped_dyf.printSchema()


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

def change_sex_with_single_char(rec):
    if str(rec["gender"]).lower() == "male":
        rec["new_gender"] = "M"
    elif str(rec["gender"]).lower() == "female":
        rec["new_gender"] = "F"
    else:
        rec["new_gender"] = "x"

    return rec

mapped_dyF =  dyf.map(f = change_sex_with_single_char)

mapped_dyF.printSchema()
mapped_dyF.show()

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Finished in {elapsed_time} seconds")
