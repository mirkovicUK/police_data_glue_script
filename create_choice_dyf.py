from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.types import *
from pyspark.sql import Row

# Initialize contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Create data with mixed types
data = [
    Row(id=1, mixed_field=123, name="John"),
    Row(id=2, mixed_field="text", name="Jane"),
    Row(id=3, mixed_field=456, name="Bob"),
    Row(id=4, mixed_field="more_text", name="Alice")
]

# Create DataFrame with mixed types
df = spark.createDataFrame(data)

# Convert to DynamicFrame (this creates choice types)
dyf = glueContext.create_dynamic_frame.from_rdd(
    df.rdd, 
    "test_choice_data"
)

# Print schema to see choice types
dyf.printSchema()