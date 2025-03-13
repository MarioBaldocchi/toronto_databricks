from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
import json

from .UrlToronto import UrlToronto


class BikeToronto:
    def __init__(self, path_json):
        self.spark = self.spark = SparkSession.getActiveSession()
        self.parametros = BikeToronto.loadParameters(path_json)

    @staticmethod
    def loadParameters(path_json):
        """
        :param path_json: path del archivo json con los parámetros necesarios para la ejecución del programa
        :return: diccionario con los parámetros necesarios para la ejecución del programa
        """
        with open(path_json) as file:
            parametros = json.load(file)
        return parametros

    def crear_df(self, year: str, month: str) -> DataFrame:
        """
        :param year: año del que se quieren consultar los datos (YY)
        :param month: mes del que se quieren consultar los datos (MM)
        :return: DataFrame de Spark con los datos del mes y año que se quieren consultar
        """
        temporal = self.parametros["temporal_path"]
        downloader = UrlToronto(temporal)
        path_csv = downloader.download_csv(year, month)

        return self.spark.read.csv(
            path_csv, sep=",", header=True, inferSchema=True, nullValue="NULL"
        )

    def procesar_df(self, df_sucio: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        :param df_sucio: DataFrame de Spark que se tiene que preprocesar.
                        Se le aplican las siguientes operaciones:
                        -Eliminación filas duplicadas
                        -Se verifica que las columnas sean del tipo correcto:
                                Start Time y End Time: fecha
                                User Type, Start Station Name y End Station Name: string
                                El resto son de tipo entero.
                        -Seleccionar las siguientes columnas:
                                Trip Duration
                                Start Station Id
                                Start Time
                                Start Station Name
                                End Station Id
                                End Time
                                End Station Name
                                Bike Id
                        -Trip Duration se expresa en segundos (y se añade una nueva columna con la duración en minutos)
        :return: 1 DataFrame de Spark con los datos de los usuarios casuales y otro con el resto de tipo de usuarios
        """
        df_final = df_sucio.dropDuplicates() # Eliminación de filas duplicadas
        df_final = df_final.withColumn('Start Time', f.to_timestamp(df_final["Start Time"], "MM/dd/yyyy HH:mm")) # Formato fecha
        df_final = df_final.withColumn('End Time', f.to_timestamp(df_final["End Time"], "MM/dd/yyyy HH:mm"))# Formato fecha
        df_final = df_final.withColumn(
            "Trip Duration Minutos", df_final["Trip  Duration"] / 60
        )

        miembro_casual = df_final["User Type"] == "Casual Member"

        return df_final.filter(miembro_casual).select(
            "Trip  Duration",
            "Trip Duration Minutos",
            "Start Station Id",
            "Start Time",
            "Start Station Name",
            "End Station Id",
            "End Time",
            "End Station Name",
            "Bike Id",
        ), df_final.filter(~miembro_casual).select(
            "Trip  Duration",
            "Trip Duration Minutos",
            "Start Station Id",
            "Start Time",
            "Start Station Name",
            "End Station Id",
            "End Time",
            "End Station Name",
            "Bike Id",
        )
