import unittest
from unittest.mock import patch, MagicMock
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from src.police_data_glue import read_csv_to_dyf, resolve_schema, main



def test_read_csv_to_dyf():
    """Test the read_csv_to_dyf function"""
    mock_context = MagicMock()
    mock_context.create_dynamic_frame.from_catalog.return_value =\
    'RETURN VALUE'

    result = read_csv_to_dyf(mock_context, "test_db", "test_table")
    # Assertions
    assert result == 'RETURN VALUE'
    mock_context.create_dynamic_frame.from_catalog.assert_called_once_with(
        database="test_db",
        table_name="test_table"
    )



@patch('src.police_data_glue.ResolveChoice.apply')
def test_ResolveChoice_called(mock_ResolveChoice):
    mock_dyf = 'TEST'
    # mock_ResolveChoice.return_value = MagicMock()
   
    resolve_schema(mock_dyf, "test_db", "test_table")
    
    mock_ResolveChoice.assert_called_once_with(
        frame=mock_dyf,
        choice="match_catalog",
        database="test_db",
        table_name="test_table"
    )


#INTEGRATION TESTS MAIN  <<<---- TO BE split in smaller tests
@patch('src.police_data_glue.resolve_schema')
@patch('src.police_data_glue.Job')
@patch('src.police_data_glue.GlueContext')
@patch('src.police_data_glue.read_csv_to_dyf')
@patch('src.police_data_glue.sys.argv')
@patch('src.police_data_glue.getResolvedOptions')
def test_integration_main(mock_getResolvedOptions,
                          mock_argv,
                          mock_read_csv_to_dyf,
                          mock_GlueContext,
                          mock_Job,
                          mock_resolve_schema):

    mock_getResolvedOptions.return_value = {
        'DATABASE': 'test_db',
        'TABLE': 'test_table',
        'JOB_NAME': 'police_data_job',
    }
    glueContext = MagicMock()
    mock_GlueContext.return_value = glueContext
    mock_Job.return_value = MagicMock()
    mock_GlueContext.spark_sesion.return_value = MagicMock()
    mock_read_csv_to_dyf.return_value = 'TEST'
    mock_resolve_schema.return_value = 'schema resolve'

    main()

    # Assertions

    #get resolved options is called with the right args
    mock_argv.__getitem__.return_value = 'test'
    mock_getResolvedOptions.assert_called_once_with(
        mock_argv,
        ['DATABASE', 'TABLE']
    )

    #read_csv_to_dyf is called with the right args
    mock_GlueContext.assert_called_once()
    mock_read_csv_to_dyf.assert_called_once_with(
        glueContext,  # This will be the GlueContext created in main
        'test_db',
        'test_table'
    )

    #mock_resolve schema is called once with right params
    mock_resolve_schema.assert_called_once_with(
        'TEST',
        'test_db',
        'test_table'
    )


