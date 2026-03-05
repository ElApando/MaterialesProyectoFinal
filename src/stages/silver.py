
"""
Docstring for src.flow.extract_data

python -m src.flow.extract_data
"""

import os
from pathlib import Path

from typing import Dict, List, cast
import pandas as pd

from config.static import DI_SCOPE, DI_COLUMNS, DI_DIMENSION, DI_REPLACE
from src.utils.tools import DataManage, write_logs


class SilverProcess:
    """ Proceso Plata """

    def __init__(self)->None:
        """ Proceso Plata

        Tiene como objetivo dar orden y formato a todos los datos de las diferentes tablas,
        para ellose extrae la información de la ubicación de silver, posteriormente se ordena 
        la información en tipo de datos, congruencia con el nombre de la columna, etc. Prosiguiendo
        se normaliza la tabla para verificar la integridad referencial de la información, por lo 
        que se revisa:
        - Tablas existentes
        - Columnas correctas
        - Tipos de datos
        - Llaves primarias
        - Llaves foraneas
        - índices 
        """

        self.ac_data = DataManage()
        self.st_path_ori: Path = DI_SCOPE["brz"]["path"]
        self.st_path_fin: Path = DI_SCOPE["slv"]["path"]
        self.di_columns =  DI_COLUMNS
        self.di_dimension = DI_DIMENSION
        self.di_replace = DI_REPLACE
        self.di_save: Dict[str, pd.DataFrame] = {}
        self.di_order: Dict[str, pd.DataFrame] = {}

    def execute(self)->None:
        """ Ejecución

        - Extracción de datos
        - Orden de tablas
        - Normalización de tablas
        - Escritura de tablas 
        """
        write_logs("[START] - Ejecución Silver")

        self.extract_data()
        df_data: pd.DataFrame = self.order_data()
        self.normalize(df_data)
        self.write_tables(df_data=df_data, st_path="data_complete.csv", st_type="D")
        df_fact: pd.DataFrame = self.fact_table(df_data)
        self.write_tables(df_data=df_fact, st_path="data_fact.csv", st_type="D")

        write_logs("[END] - Ejecución Silver")

    def fact_table(self, df_data:pd.DataFrame)->pd.DataFrame:
        """ Tabla de Hechos
        
        La tabla de hechos contiene toda la información obtenida de las diferentes 
        tablas, se asigna claves para identificar de forma rapida y facil la infformación

        Parameters:
            df_data (DataFrame): Tabla con toda la información correspondiente

        Returns:
            df_fact (DataFrame): Tabla desglosada con las claves correspondientes a
            cada una de las dimensiones.
        """
        write_logs("[INFO] - Tabla de hechos")
        df_canal: pd.DataFrame = self.di_save["dim_canal"]
        df_puesto: pd.DataFrame = self.di_save["dim_puesto"]
        df_producto: pd.DataFrame = self.di_save["dim_producto"]
        df_categoria: pd.DataFrame = self.di_save["dim_categoria"]
        df_metodo_pago: pd.DataFrame = self.di_save["dim_metodo_pago"]

        df_fact: pd.DataFrame = (df_data.merge(df_puesto, on="puesto")
        .merge(df_canal, on="canal")
        .merge(df_categoria, on="categoria")
        .merge(df_producto, on="producto")
        .merge(df_metodo_pago, on="metodo_pago")[["id_venta", "fecha", "hora", "id_puesto",
         "id_producto", "id_categoria", "id_canal", "id_metodo_pago", "precio"]])
        df_fact = df_fact.drop_duplicates()

        return df_fact

    def normalize(self, df_data:pd.DataFrame)->None:
        """Normalización de Tabla

        Recuerda que la normalización de la tabla es separar la informaicón en diferentes tablas
        con la finalidad de verificar la unisidad de los datos
        
        Parameters:
            - df_data (DataFrame): Tabla con toda la información correspondiente
        """
        write_logs("[INFO] - Normalización de Tabla")
        for dimension in self.di_dimension["dimension"]:
            df_new = (
                df_data[[dimension]]
                .drop_duplicates()
                .reset_index(drop=True)
                .assign(puesto_id=lambda x, dim=dimension: f"{dim}_" + (x.index + 1).astype(str))
            )
            df_new = df_new.rename(columns = {"puesto_id": f"id_{dimension}"})
            df_new.to_csv(f"{self.st_path_fin}/dim_{dimension}.csv", index = False)
            self.di_save[f"dim_{dimension}"] = df_new

    def extract_data(self)->None:
        """ Extracción de información 

        La extracción se obtiene de la carpeta bronze, se extraen todos los archivos
        sin distinción por lo que es importante que no haya archivos extraños. 
        """
        write_logs("[INFO] - Extración de datos")
        ls_files: List[str] = os.listdir(self.st_path_ori)

        for file in ls_files:
            st_path = self.st_path_ori / file
            df_data: pd.DataFrame = pd.read_csv(st_path) # type: ignore
            self.di_order[file] = df_data

    def order_data(self)->pd.DataFrame:
        """ Orden y Limpieza de datos

        Es lo importante de Silver todos los datos se limpian y se les asigna el formato
        correspondiente para que se puedan utilziar en gold y en la visualización

        Returns:
            df_concat (DataFrame): DataFrame con toda la información proveniente de los diferentes
            archivos.
        """
        write_logs("[INFO] - Orden de datos")
        df_concat = pd.DataFrame()

        for _, data in self.di_order.items():
            data[self.di_columns["time"]] = (
            data[self.di_columns["time"]].str.replace("AM","").str.replace("PM","")) # type:ignore
            data[self.di_columns["add"][0]] = (data[self.di_columns["time"]].
            str.extract(r"(\d{1,4}[/-]\d{1,2}[/-]\d{1,4})"))#type:ignore
            data[self.di_columns["add"][1]] = (
            data[self.di_columns["time"]].str.extract(r"(\d{1,2}[:]\d{1,2})")) # type:ignore

            for tipe, columns in self.di_columns["type_columns"].items():
                data =  self.ac_data.format_column(data, columns, tipe)

            df_data: pd.DataFrame = data[cast(List[str], self.di_columns["order_final"])]
            df_concat = pd.concat([df_concat, df_data], ignore_index=True) # type:ignore

            for key in self.di_replace["unify"]:
                df_concat[key] = (df_concat[key].
                                replace(self.di_replace["unify"][key])) # type:ignore

        return df_concat

    def write_tables(self, st_path:str, st_type:str, df_data:pd.DataFrame|None=None)->None:
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
            df_data.to_csv(f"{self.st_path_fin}/{st_path}", index=False) # type:ignore

        else:
            assert ValueError("Esa opcion no existe!")

# Fintie Incantatem
