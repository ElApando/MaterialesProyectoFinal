"""
Docstring for src.flow.extract_data

python -m src.flow.extract_data
"""

import os
from pathlib import Path

from typing import Dict, Any
import pandas as pd

from config.static import DI_SCOPE, DI_COLUMNS, DI_REPLACE
from src.utils.tools import DataManage, write_logs

class BronzeProcess:
    """ Proceso Bronce """

    def __init__(self)->None:
        """Proceso Bronce
        
        Lleva a cabo la extracción de los datos y les daformato, con la finalidad de que se
        procese la información de forma adecuada. 
        """
        self.ac_data = DataManage()
        self.st_path_ori: Path = DI_SCOPE["raw"]["path"]
        self.st_path_fin: Path = DI_SCOPE["brz"]["path"]
        self.di_columns = DI_COLUMNS
        self.di_replace = DI_REPLACE
        self.di_order: Dict[str, pd.DataFrame] = {}
        self.ls_drop = ["README.txt"]

    def execute(self)->None:
        """ Ejecución

        - Extrae la información
        - Orden las columnas y las filas 
        - Escribe en la ruta correspondiente a Bronze
        """
        write_logs("[START] - Ejecución Bronze")
        self.extract_data()
        self.order_columns()
        self.write_tables()
        write_logs("[END] - Ejecución Bronze")

    def extract_data(self)->None:
        """ Extracción de datos

        Extrae los datos de la carpeta RAW, revisa que tipo de saparador contienen los datos
        y coloca el mismo en pandas para poder extraer la data de forma correcta
        """
        write_logs("[INFO] - Extración de datos")
        df_data: pd.DataFrame = pd.DataFrame()

        ls_files = os.listdir(self.st_path_ori)
        ls_files = [file for file in ls_files if file not in self.ls_drop]

        for file in ls_files:
            st_type = file.split(".")[-1]
            st_path = self.st_path_ori / file

            if st_type == "csv" or st_type == "txt":
                st_separator = self.ac_data.separator_table(st_path)
                df_data: pd.DataFrame = pd.read_csv(st_path, sep=st_separator) # type: ignore

            if st_type == "xlsx":
                df_data: pd.DataFrame = pd.read_excel(st_path) # type: ignore

            if st_type == "sqlite":
                di_tables: Dict[str, Any] = self.ac_data.extract_tables_sqlite(st_path)
                self.di_order.update(di_tables)

            if not df_data.empty:
                self.di_order[file] = df_data

    def _verify_row(self, se_row: pd.Series, st_column: str)->pd.Series|str:
        """ Verificación de fila

        Revisa que la fila tenga las columnas requeridas

        Parameters:
            st_row (str): Fila en cuestión que debe de ser revisada
        Returns:
            ls_row (str): Lista con los datos correspondientes 
        """
        try:
            st_separator = self.ac_data.separator_what(se_row[st_column])
            ls_row = se_row[st_column].split(st_separator)
            se_row[:] = ls_row
            return se_row

        except (ValueError, TypeError):
            return se_row

    def _apply_verify(self, row: pd.Series) -> pd.Series|str:
        """ Función que simplifica el uso de apply, creo que es más
        comodo trabajar de este modo
        """
        se_return = self._verify_row(row, self.di_columns["id"])
        return se_return

    def order_columns(self)->None:
        """ Orden de columnas

        Revisa que las columnas no esten contaminadas por cadenas de texto, esto 
        se lelva a cabo por la naturaleza de lso datos.
        """
        write_logs("[INFO] - Orden de columnas")
        for name, data in self.di_order.items():
            self.di_order[name] = data.apply(self._apply_verify, axis=1) # type: ignore

    def write_tables(self)->None:
        """ Escritura de tablas

        Escribe las tablas en la ubicación requerida en formato de csv
        """
        write_logs("[INFO] - Escritura de tablas correspondientes")
        in_count = 0

        for _, data in self.di_order.items():
            data.to_csv(f"{self.st_path_fin}/data_{in_count}.csv", index=False)
            in_count = in_count + 1

# Finite Incantatem
