from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

# Create JSON data with mixed types
json_data = [
    '{"id": 1, "value": 100}',
    '{"id": 2, "value": "string"}',
    '{"id": 3, "value": 200}',
    '{"id": 4, "value": "another_string"}'
]

# Create RDD and DynamicFrame
rdd = sc.parallelize(json_data)
dyf = glueContext.create_dynamic_frame.from_rdd(rdd, "json_choice_data")

dyf.printSchema()  # Shows choice type for 'value' field