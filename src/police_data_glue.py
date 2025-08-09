import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def read_csv_to_dyf(glueContext, database, table):
    """
    Reads a CSV file and returns a DynamicFrame.

    Args:
        glueContext (GlueContext): The Glue context object.
        database (str): DataCatalog databse name
        table (str): The name of the table in the DataCatalog.

    Returns:
        DynamicFrame: The DynamicFrame object representing the CSV data.
    """
    # Read the CSV file into a DynamicFrame
    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table)
    return dyf

def resolve_schema(dyf, args):
    """
    Resolves the schema of a DynamicFrame to match the catalog.

    Args:
        dyf (DynamicFrame): The DynamicFrame to resolve.

    Returns:
        DynamicFrame: The resolved DynamicFrame.
    """
    resolved_dyf = ResolveChoice.apply(
    frame=dyf,
    choice="match_catalog",
    database=args['--database'],
    table_name=args['--table']
    )
    return resolved_dyf


if __name__ == "__main__":
    params = []
    if '--JOB_NAME' in sys.argv:
        params.append('JOB_NAME')
    args = getResolvedOptions(sys.argv, params)

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    if 'JOB_NAME' in args:
        jobname = args['JOB_NAME']
    else:
        jobname = "police_data_job"
    job.init(jobname, args)

    #get logger for this glue job
    logger = glueContext.get_logger()
    logger.info(f"Job {jobname} started with args: {args}")

    dyf = read_csv_to_dyf(glueContext, args['--database'], args['--table'])

    dyf = resolve_schema(dyf, args)