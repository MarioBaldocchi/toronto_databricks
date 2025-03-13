import requests
import re
import zipfile
import os
import io

class UrlToronto:
    BASE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

    def __init__(self, ubicacion_temporal):
        """
        :param ubicacion_temporal: ubicacion donde se guardará el fichero CSV descomprimido
        """
        self._temporal_path_ = ubicacion_temporal
        self.valid_urls = UrlToronto.select_valid_urls()

    @staticmethod
    def select_valid_urls():
        """
        :return: lista con las urls validas para la descarga de los datos

        Excepciones
        :ConnectionError: Error al intentar conectar con el servidor
        """
        url = UrlToronto.BASE_URL + "/api/3/action/package_show"
        params = {"id": "bike-share-toronto-ridership-data"}
        package = requests.get(url, params=params).json()

        if not package["success"]:
            raise ConnectionError("Error conexión")

        url_validas = []
        for _, resource in enumerate(package["result"]["resources"]):
            # To get metadata for non datastore_active resources:
            if not resource["datastore_active"]:
                url = (
                    UrlToronto.BASE_URL
                    + "/api/3/action/resource_show?id="
                    + resource["id"]
                )
                resource_metadata = requests.get(url).json()
                if (resource_metadata["success"]) and (
                    re.findall(
                        "bikeshare-ridership-\d{4}.zip$",
                        resource_metadata["result"]["url"],
                    )
                ):
                    url_validas.append(resource_metadata["result"]["url"])
        return url_validas

    def download_csv(self, year: str, month: str) -> str:
        """
        :param year: año del que se quieren consultar los datos (YY)
        :param month: mes del que se quieren consultar los datos (MM)
        :return: ruta donde se guarda el csv

        Excepciones
        :ConnectionError: Error al intentar conectar con el servidor
        :ValueError: 1) Si mes o año fuera de rango. 2) Si no se encuentra la url segun los parametros
        """
        # temporal_path: ruta elegida donde se guardará el fichero CSV descomprimido
        temporal_path = (
            self._temporal_path_
        )  # ruta donde se guardará el fichero CSV descomprimido
        # url: será la url donde se encuentran los datos (para year = 2023, esta url se corresponde con un ZIP con 12 ficheros, uno para cada mes )
        if not re.findall("(^0[1-9]$)|(^1[0-2]$)", month) or not re.findall(
            "^202[0-3]$", year
        ):
            raise ValueError("Compruebe que `month` entre 1 y 12, `year` entre 20 y 23")
        url = None
        for u in self.valid_urls:
            if re.findall(f"bikeshare-ridership-{year}.zip", u):
                url = u
        if url is None:
            raise ValueError("url no encontrada")
        # name: nombre del fichero que nos intersa ( puede ser parte del nombre, algo de tipo '2023-05.csv')
        name = f"{year}-{month}"  # nombre del fichero que nos intersa ( algo de tipo '2023-05.csv')
        response = requests.get(url)  # petición

        if response.status_code == 200:
            content = io.BytesIO(response.content)
            zfile = zipfile.ZipFile(content, "r")
            files = [
                f for f in zfile.filelist if f.filename.find(name) > 0
            ]  # devuelve una lista unitaria con el fichero que nos interesa

            # Extraemos el contenido en una carpeta temporal
            #dbutils.fs.mkdirs(f"dbfs:{temporal_path}")  # Creamos la carpeta temporal
            zfile.extractall(
                temporal_path, members=files
            )  # extraemos del ZIP solo el fichero que nos interesa

            # nombre del fichero csv :
            file_path = os.path.join(temporal_path, files[0].filename) 
            return file_path
        else:
            raise ConnectionError("Error conexión")

