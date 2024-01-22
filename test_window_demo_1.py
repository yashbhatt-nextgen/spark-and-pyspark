import tempfile
import os
import pytest
from pyspark.sql import SparkSession
from window_demo_1 import perform_window_operation

@pytest.fixture
def spark():
    return SparkSession.builder.appName("test").getOrCreate()

def test_perform_window_operation(spark):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
        temp_file.write(b"IMSI,Activation_date,account_number\n915,29-01-2023,7\n507,13-07-2023,7\n899,16-10-2023,9\n"
                        b"489,25-05-2023,5\n591,11-01-2023,1\n343,14-10-2023,9\n756,09-05-2023,9\n597,10-02-2023,7\n"
                        b"168,07-05-2023,1\n758,06-12-2023,2\n")

    try:
        result_df, spark_session = perform_window_operation(temp_file.name)

        expected_columns = ['IMSI', 'Activation_date', 'account_number', 'IMSI_count_per_account']
        assert result_df.columns == expected_columns
        assert result_df.filter((result_df['account_number'] == 7) & (result_df['Activation_date'] == '29-01-2023')).count() == 1
        assert result_df.filter((result_df['account_number'] == 7) & (result_df['Activation_date'] == '13-07-2023')).count() == 1
        expected_count = 2
        actual_count = result_df.filter((result_df['account_number'] == 7)).collect()[1]['IMSI_count_per_account']
        print("-----------------")
        print(actual_count)

        assert result_df.filter((result_df['account_number'] == 7)).collect()[1]['IMSI_count_per_account'] == 2
        assert result_df.filter((result_df['account_number'] == 7)).collect()[2]['IMSI_count_per_account'] == 3


    finally:
        os.remove(temp_file.name)
