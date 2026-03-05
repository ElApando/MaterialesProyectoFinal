"""
Docstring for src.flow.extract_data

python -m src.flow.extract_data
"""

import os
from pathlib import Path

from typing import Dict
import pandas as pd

from config.static import DI_SCOPE
from src.utils.tools import write_logs

class GoldProcess:
    """ Proceso Oro """

    def __init__(self):
        """ Proceso Oro

        El proceso desnormaliza la informaicón, aplica las reglas de negoció y perfila los datos
        para poder trabajar de forma rapida en BigQuery
        """
        self.st_path_ori: Path = DI_SCOPE["slv"]["path"]
        self.st_path_fin: Path = DI_SCOPE["gld"]["path"]
        self.di_order: Dict[str, pd.DataFrame] = {}

    def execute(self):
        """ Ejecución

        - Extracción de la información correspondiente
        - Desnormalización de las tablas
        - Reglas de negocio
        - Escritura de tablas
        """
        write_logs("[START] - Ejecución Gold")
        self.extract_data()
        df_data = self.denormalize()
        df_data = self.rules(df_data)
        self.write_tables(df_data, "sale_product.csv", "d")
        write_logs("[END] - Ejecución Gold")

    def extract_data(self):
        """ Extracción de información 

        La extracción se obtiene de la carpeta silver, se obtienen las dimensiones y la tabla de
        hecho. 
        """
        write_logs("[INFO] - Extracción de datos")
        ls_files = os.listdir(self.st_path_ori)

        for file in ls_files:
            if "dim" in file or "fact" in file:
                st_name = file[:-4]
                st_path = self.st_path_ori / file
                df_data = pd.read_csv(st_path) # type: ignore
                self.di_order[st_name] = df_data

    def denormalize(self)->pd.DataFrame:
        """Desnormalización de Tabla

        Se unen las diemensiones para lograr crear una sola tabla verificada
        """
        write_logs("[INFO] - Desnormalización de tablas")
        df_data = self.di_order["data_fact"]

        for name, data in self.di_order.items():
            if not "fact" in name:
                st_name = name[4:]
                df_data = df_data.merge(data, on=f"id_{st_name}")

        df_data = df_data[["id_venta", "fecha", "hora", "puesto", "producto", "categoria", "canal",
                           "metodo_pago", "precio"]]

        return df_data

    def rules(self, df_data:pd.DataFrame)->pd.DataFrame:
        """ Reglas de negocio

        - El costo de ticket debe de ser mayor a 0

        Parameters:
            df_data (DataFrame): DataFrame sin reglas de negocio
        Returns:
            df_data (DataFrame): DataFrame con reglas de negocio
        """
        write_logs("[INFO] - Reglas de negocio")
        df_data = df_data.loc[df_data["precio"] > 0]

        return df_data

    def write_tables(self, df_data:pd.DataFrame, st_path:str, st_type:str)->None:
        """ Escritura de tablas

        Escribe las tablas en la ubicación requerida en formato de csv, es importante
        resaltar que se puede escribir un grupo de DataFrames o un solo DataFrame sin 
        problema alguno, sólo se debera de seleccionar la opción correspondiente, T para
        distintas tablas, D para una sola

        Parameters:
            df_data (DataFrame): DataFrame con la información de importancia 
            st_path (str): Nombre del archivo, el tipo de archivo debe de ser csv
            st_type (str): Tipo de guardado, T varias tablas, D una Tabla
        """
        write_logs("[INFO] - Escritura de tabla")
        st_type = st_type.upper()

        if st_type == "T":
            in_count = 0

            for _, data in self.di_order.items():
                data.to_csv(f"{self.st_path_fin}/data_{in_count}.csv", index=False)
                in_count = in_count + 1

        elif st_type == "D":
            df_data.to_csv(f"{self.st_path_fin}/{st_path}", index=False)

        else:
            assert ValueError("Esa opcion no existe!")
