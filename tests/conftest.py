import pytest
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# # Add any common fixtures or configuration here
# @pytest.fixture(autouse=True)
# def mock_aws_imports():
#     """Auto-mock AWS Glue imports to avoid import errors in test environment"""
#     with patch.dict('sys.modules', {
#         'awsglue': patch('awsglue').start(),
#         'awsglue.transforms': patch('awsglue.transforms').start(),
#         'awsglue.utils': patch('awsglue.utils').start(),
#         'awsglue.context': patch('awsglue.context').start(),
#         'awsglue.job': patch('awsglue.job').start(),
#         'pyspark': patch('pyspark').start(),
#         'pyspark.context': patch('pyspark.context').start(),
#     }):
#         yield

@pytest.fixture(scope="session")
def glueContext():
    """
    Function to setup test environment for PySpark and Glue
    """
    spark_context = SparkContext()
    glueContext = GlueContext(spark_context)
    yield glueContext
    spark_context.stop()