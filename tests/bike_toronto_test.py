import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from toronto_bike.BikeToronto import BikeToronto

@pytest.fixture
def bike_toronto():
    with patch('toronto_bike.BikeToronto.BikeToronto.loadParameters') as mock_load_params:
        mock_load_params.return_value = {"temporal_path": "temp_dir"}
        return BikeToronto("parametros.json")

@patch('toronto_bike.BikeToronto.UrlToronto')
def test_crear_df(mock_url_toronto, bike_toronto, spark):
    mock_url_toronto_instance = mock_url_toronto.return_value
    mock_url_toronto_instance.download_csv.return_value = "path/to/csv"

    with patch.object(spark.read, 'csv') as mock_read_csv:
        mock_df = MagicMock()
        mock_read_csv.return_value = mock_df

        df = bike_toronto.crear_df("23", "05")
        mock_read_csv.assert_called_once_with("path/to/csv", sep=",", header=True, inferSchema=True, nullValue="NULL")
        assert df == mock_df

def test_procesar_df(bike_toronto, spark):
    data = [
        ("1", "01/01/2023 10:00", "01/01/2023 10:30", "Casual Member", "Station A", "Station B", 1, 2, "Bike 1"),
        ("2", "01/01/2023 11:00", "01/01/2023 11:20", "Anual Member", "Station C", "Station D", 3, 4, "Bike 2")
    ]
    columns = ["Trip  Duration", "Start Time", "End Time", "User Type", "Start Station Name", "End Station Name", "Start Station Id", "End Station Id", "Bike Id"]
    df_sucio = spark.createDataFrame(data, columns)

    df_casual, df_anual = bike_toronto.procesar_df(df_sucio)

    assert df_casual.count() + df_anual.count() == df_sucio.count()

    fila_casual = df_casual.collect()[0]
    fila_anual = df_anual.collect()[0]

    assert fila_casual["User Type"] == "Casual Member"
    assert fila_anual["User Type"] == "Anual Member"

if __name__ == '__main__':
    pytest.main()
