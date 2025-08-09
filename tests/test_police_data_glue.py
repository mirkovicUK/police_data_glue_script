import pytest
from unittest.mock import patch, MagicMock
import sys
from awsglue.utils import getResolvedOptions

import pytest
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys
from src.police_data_glue import read_csv_to_dyf


def test_read_csv_to_dyf():
    """Test the read_csv_to_dyf function"""
    mock_context = MagicMock()
    mock_dyf = MagicMock()
    mock_context.create_dynamic_frame.from_catalog.return_value = mock_dyf

    result = read_csv_to_dyf(mock_context, "test_db", "test_table")
    # Assertions
    assert result == mock_dyf
    assert result is not mock_context
    mock_context.create_dynamic_frame.from_catalog.assert_called_once_with(
        database="test_db",
        table_name="test_table"
    )

class TestJobNameHandling:
    
    @patch('sys.argv', ['script.py', '--JOB_NAME', 'test_job'])
    @patch('awsglue.utils.getResolvedOptions')
    def test_job_name_provided(self, mock_get_options):
        mock_get_options.return_value = {'JOB_NAME': 'test_job'}
        
        params = []
        if '--JOB_NAME' in sys.argv:
            params.append('JOB_NAME')
        args = getResolvedOptions(sys.argv, params)
        assert 'JOB_NAME' in params
        assert args['JOB_NAME'] == 'test_job'
    
    @patch('sys.argv', ['script.py'])
    @patch('awsglue.utils.getResolvedOptions')
    def test_job_name_not_provided(self, mock_get_options):
        mock_get_options.return_value = {}
        
        params = []
        if '--JOB_NAME' in sys.argv:
            params.append('JOB_NAME')
        args = getResolvedOptions(sys.argv, params)
        
        assert len(params) == 0
        assert 'JOB_NAME' not in args
    
#     def test_default_job_name(self):
#         args = {}
#         jobname = args['JOB_NAME'] if 'JOB_NAME' in args else "police_data_job"
#         assert jobname == "police_data_job"
    
#     def test_custom_job_name(self):
#         args = {'JOB_NAME': 'custom_job'}
#         jobname = args['JOB_NAME'] if 'JOB_NAME' in args else "police_data_job"
#         assert jobname == "custom_job"


# class TestJobInitialization:
    
#     def test_job_init_called_with_correct_params(self, mock_glue_components):
#         job_instance = mock_glue_components['job'].return_value
        
#         jobname = "test_job"
#         args = {'JOB_NAME': 'test_job'}
        
#         job_instance.init(jobname, args)
#         job_instance.init.assert_called_once_with("test_job", args)
    
#     def test_job_init_with_default_name(self, mock_glue_components):
#         job_instance = mock_glue_components['job'].return_value
        
#         jobname = "police_data_job"
#         args = {}
        
#         job_instance.init(jobname, args)
#         job_instance.init.assert_called_once_with("police_data_job", args)


# class TestLogging:
    
#     def test_logger_creation(self, mock_glue_components):
#         glue_context = mock_glue_components['glue_context'].return_value
#         logger = glue_context.get_logger()
        
#         glue_context.get_logger.assert_called_once()
#         assert logger is not None
    
#     def test_logger_info_called(self, mock_glue_components):
#         logger = mock_glue_components['logger']
#         jobname = "test_job"
#         args = {'JOB_NAME': 'test_job'}
        
#         logger.info(f"Job {jobname} started with args: {args}")
#         logger.info.assert_called_once_with("Job test_job started with args: {'JOB_NAME': 'test_job'}")


# @pytest.mark.parametrize("argv,expected_params,expected_jobname", [
#     (['script.py'], [], "police_data_job"),
#     (['script.py', '--JOB_NAME', 'custom'], ['JOB_NAME'], "custom"),
#     (['script.py', '--OTHER_PARAM', 'value'], [], "police_data_job"),
# ])
# def test_parameter_parsing(argv, expected_params, expected_jobname):
#     with patch('sys.argv', argv):
#         with patch('awsglue.utils.getResolvedOptions') as mock_get_options:
#             mock_get_options.return_value = {'JOB_NAME': 'custom'} if expected_jobname == 'custom' else {}
            
#             params = []
#             if '--JOB_NAME' in sys.argv:
#                 params.append('JOB_NAME')
#             args = mock_get_options(sys.argv, params)
            
#             jobname = args['JOB_NAME'] if 'JOB_NAME' in args else "police_data_job"
            
#             assert params == expected_params
#             assert jobname == expected_jobname


# class TestIntegration:
    
#     @patch('sys.argv', ['script.py', '--JOB_NAME', 'integration_test'])
#     @patch('awsglue.utils.getResolvedOptions')
#     def test_full_initialization_flow(self, mock_get_options, mock_glue_components):
#         mock_get_options.return_value = {'JOB_NAME': 'integration_test'}
        
#         # Simulate the full flow
#         params = []
#         if '--JOB_NAME' in sys.argv:
#             params.append('JOB_NAME')
#         args = mock_get_options(sys.argv, params)
        
#         glue_context = mock_glue_components['glue_context'].return_value
#         job_instance = mock_glue_components['job'].return_value
#         logger = mock_glue_components['logger']
        
#         jobname = args['JOB_NAME'] if 'JOB_NAME' in args else "police_data_job"
        
#         # Verify all components are called correctly
#         assert jobname == 'integration_test'
#         job_instance.init(jobname, args)
#         logger.info(f"Job {jobname} started with args: {args}")
        
#         job_instance.init.assert_called_once_with('integration_test', args)
#         logger.info.assert_called_once()


# if __name__ == "__main__":
#     pytest.main([__file__])



# import pytest
# from unittest.mock import patch, MagicMock

# class TestResolveSchema:
    
#     @patch('awsglue.transforms.ResolveChoice')
#     def test_resolve_schema_success(self, mock_resolve_choice):
#         # Setup
#         mock_dyf = MagicMock()
#         mock_resolved_dyf = MagicMock()
#         mock_resolve_choice.apply.return_value = mock_resolved_dyf
        
#         args = {
#             '--database': 'test_db',
#             '--table': 'test_table'
#         }
        
#         # Test
#         result = resolve_schema(mock_dyf, args)
        
#         # Verify
#         mock_resolve_choice.apply.assert_called_once_with(
#             frame=mock_dyf,
#             choice="match_catalog",
#             database='test_db',
#             table_name='test_table'
#         )
#         assert result == mock_resolved_dyf
    
#     @patch('awsglue.transforms.ResolveChoice')
#     def test_resolve_schema_with_different_args(self, mock_resolve_choice):
#         mock_dyf = MagicMock()
#         mock_resolved_dyf = MagicMock()
#         mock_resolve_choice.apply.return_value = mock_resolved_dyf
        
#         args = {
#             '--database': 'police_db',
#             '--table': 'incidents'
#         }
        
#         result = resolve_schema(mock_dyf, args)
        
#         mock_resolve_choice.apply.assert_called_once_with(
#             frame=mock_dyf,
#             choice="match_catalog",
#             database='police_db',
#             table_name='incidents'
#         )
#         assert result == mock_resolved_dyf
    
#     @patch('awsglue.transforms.ResolveChoice')
#     def test_resolve_schema_exception(self, mock_resolve_choice):
#         mock_dyf = MagicMock()
#         mock_resolve_choice.apply.side_effect = Exception("Schema resolution failed")
        
#         args = {
#             '--database': 'test_db',
#             '--table': 'test_table'
#         }
        
#         with pytest.raises(Exception, match="Schema resolution failed"):
#             resolve_schema(mock_dyf, args)
    
#     @pytest.mark.parametrize("database,table", [
#         ("db1", "table1"),
#         ("police_db", "incidents"),
#         ("warehouse", "fact_table")
#     ])
#     @patch('awsglue.transforms.ResolveChoice')
#     def test_resolve_schema_parametrized(self, mock_resolve_choice, database, table):
#         mock_dyf = MagicMock()
#         mock_resolved_dyf = MagicMock()
#         mock_resolve_choice.apply.return_value = mock_resolved_dyf
        
#         args = {
#             '--database': database,
#             '--table': table
#         }
        
#         result = resolve_schema(mock_dyf, args)
        
#         mock_resolve_choice.apply.assert_called_once_with(
#             frame=mock_dyf,
#             choice="match_catalog",
#             database=database,
#             table_name=table
#         )
#         assert result == mock_resolved_dyf
    
#     def test_resolve_schema_missing_args(self):
#         mock_dyf = MagicMock()
#         args = {}  # Missing required args
        
#         with pytest.raises(KeyError):
#             resolve_schema(mock_dyf, args)
