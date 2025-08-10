import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

s3_bucket_path = 's3://uros75-police-data/cleaned/'

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

def clean_data(df, drop):
    """
    Cleans the data by dropping col from drop.

    Args:
        df (DataFrame): The DataFrame to be cleaned.

    Returns:
        DataFrame: The cleaned DataFrame.
    """
    df = df.drop(*drop)

    return df

def resolve_schema(dyf, database, table):
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
    database=database,
    table_name=table
    )
    return resolved_dyf

def write_data(glueContext, dyf, database, table):
    """
    Writes the DynamicFrame to S3 and updates the DataCatalog.

    Args:
        glueContext (GlueContext): The Glue context object.
        dyf (DynamicFrame): The DynamicFrame to write.
        database (str): The name of the DataCatalog database.
        table (str): The name of the table in the DataCatalog.
    """
    s3output = glueContext.getSink(
    path=s3_bucket_path,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output",
    )
    s3output.setCatalogInfo(
    catalogDatabase=database,
    catalogTableName=f'{table}_cleaned'
    )
    s3output.setFormat("glueparquet")
    s3output.writeFrame(dyf)

def main():
    params = ['DATABASE', 'TABLE']
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

    dyf = read_csv_to_dyf(glueContext, args['DATABASE'], args['TABLE'])

    dyf = resolve_schema(dyf, args['DATABASE'], args['TABLE'])

    df = dyf.toDF()

    col_to_drop = [
        'op01',
        'op02',
        'op03',
        'cl01',
        'cl02',
        'cl03',
        'close_type_3',
        'ward',
        'ward_code',
        'response_time',
        'duplicate',
        'asbcount',
        'datetime',
        'ward_wardcode',
        'safer_neighborhood_team_name',
        'safer_neighborhood_team_code',
        'safer_neighborhood_team_borough_name',
        'safer_neighborhood_team_borough_code']
    df = clean_data(df, col_to_drop)

    cleaned_dyf = DynamicFrame.fromDF(df, glueContext, "cleaned_dyf")

    # Write the cleaned data to S3 and DataCatalog
    write_data(glueContext, cleaned_dyf, args['DATABASE'], args['TABLE'])

if __name__ == "__main__":
    main()
