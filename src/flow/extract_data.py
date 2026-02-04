"""
Docstring for src.flow.extract_data

python -m src.flow.extract_data
"""

import os
from pathlib import Path

from typing import Dict, List
import pandas as pd, DataFrame

from config.static import DI_SCOPE, DI_COLUMNS, DI_DIMENSION, DI_REPLACE
from src.utils.tools import FolderManage, DataManage

class RawProcess:
    """DOC
    """

    def __init__(self):
        """DOC
        """
        self.ac_folder = FolderManage()
        self.st_path_ori = DI_SCOPE["ori"]["path"]
        self.st_path_raw = DI_SCOPE["raw"]["path"]

    def execute(self):
        """DOC
        """
        ls_files = os.listdir(self.st_path_ori)

        for file in ls_files:
            st_ori: str = self.st_path_ori / file
            st_raw: str = self.st_path_raw / file
            self.ac_folder.move_file(st_ori, st_raw, "copy")


class BronzeProcess:
    """ DOC
    """

    def __init__(self)->None:
        """DOC"""
        self.ac_data = DataManage()
        self.st_path_ori = DI_SCOPE["raw"]["path"]
        self.st_path_fin = DI_SCOPE["brz"]["path"]
        self.di_columns = DI_COLUMNS
        self.di_replace = DI_REPLACE
        self.di_order = {}
        self.ls_drop = ["README.txt"]

    def execute(self)->None:
        """DOC
        """
        self.extract_data()
        self.order_columns()
        self.write_tables()

    def extract_data(self)->None:
        """ DOC
        """

        ls_files = os.listdir(self.st_path_ori)
        ls_files = [file for file in ls_files if file not in self.ls_drop]

        for file in ls_files:
            st_type = file.split(".")[-1]
            st_path = self.st_path_ori / file

            if st_type == "csv" or st_type == "txt":
                st_separator = self.ac_data.separator_table(st_path)
                df_data = pd.read_csv(st_path, sep=st_separator)

            if st_type == "xlsx":
                df_data = pd.read_excel(st_path)

            if st_type == "sqlite":
                di_tables = self.ac_data.extract_tables_sqlite(st_path)
                self.di_order.update(di_tables)

            self.di_order[file] = df_data

    def _verify_row(self, df_row: str, st_column: str)->None:
        """DOC"""

        try:
            st_separator = self.ac_data.separator_what(df_row[st_column])
            ls_row = df_row[st_column].split(st_separator)
            df_row[:] = ls_row
            return df_row

        except (ValueError, TypeError):
            return df_row

    def order_columns(self)->None:
        """ DOC
        """

        for name, data in self.di_order.items():
            self.di_order[name] = data.apply(lambda row: self._verify_row(row, self.di_columns["id"]),
                                             axis = 1)

    def write_tables(self)->None:
        """ DOC
        """
        in_count = 0

        for _, data in self.di_order.items():
            data.to_csv(f"{self.st_path_fin}/data_{in_count}.csv", index=False)
            in_count = in_count + 1


class SilverProcess:
    """ DOC
    """

    def __init__(self)->None:
        """DOC"""

        # Config
        self.ac_data = DataManage()
        self.st_path_ori: Path = DI_SCOPE["brz"]["path"]
        self.st_path_fin: Path = DI_SCOPE["slv"]["path"]
        self.di_columns = DI_COLUMNS
        self.di_dimension = DI_DIMENSION
        self.di_replace = DI_REPLACE
        self.di_save: Dict[str, pd.DataFrame] = {}
        self.di_order: Dict[str, pd.DataFrame] = {}

    def execute(self)->None:
        """DOC
        """

        self.extract_data()
        df_data: pd.DataFrame = self.order_data()
        self.normalize(df_data)
        self.write_tables(df_data,"data_complete.csv","D")
        df_fact: pd.DataFrame = self.fact_table(df_data)
        self.write_tables(df_fact,"data_fact.csv","D")

    def fact_table(self, df_data:pd.DataFrame)->pd.DataFrame:
        """DOC"""

        df_canal: pd.DataFrame = self.di_save["dim_canal"]
        df_puesto: pd.DataFrame = self.di_save["dim_puesto"]
        df_producto: pd.DataFrame = self.di_save["dim_producto"]
        df_categoria: pd.DataFrame = self.di_save["dim_categoria"]
        df_metodo_pago: pd.DataFrame = self.di_save["dim_metodo_pago"]

        df_fact: pd.DataFrame = (df_data.merge(df_puesto, on="puesto")
        .merge(df_canal, on="canal")
        .merge(df_categoria, on="categoria")
        .merge(df_producto, on="producto")
        .merge(df_metodo_pago, on="metodo_pago")
        [["id_venta", "fecha", "hora", "id_puesto", "id_producto",
                                 "id_categoria", "id_canal", "id_metodo_pago", "precio"]])
        df_fact = df_fact.drop_duplicates()

        return df_fact

    def normalize(self, df_data:pd.DataFrame)->None:
        """DOC
        """

        for dimension in self.di_dimension["dimension"]:
            df_new = (
                df_data[[dimension]]
                .drop_duplicates()
                .reset_index(drop=True)
                .assign(puesto_id=lambda x: f"{dimension}_" + (x.index + 1).astype(str))
            )
            df_new = df_new.rename(columns = {"puesto_id": f"id_{dimension}"})
            df_new.to_csv(f"{self.st_path_fin}/dim_{dimension}.csv", index = False)
            self.di_save[f"dim_{dimension}"] = df_new

    def extract_data(self)->None:
        """ DOC
        """
        ls_files: List[str] = os.listdir(self.st_path_ori)

        for file in ls_files:
            st_path = self.st_path_ori / file
            df_data: DataFrame = pd.read_csv(st_path)
            self.di_order[file] = df_data

    def order_data(self)->pd.DataFrame:
        """ DOC
        """
        df_concat = pd.DataFrame()

        for _, data in self.di_order.items():
            data[self.di_columns["time"]] = (data[self.di_columns["time"]].str.replace("AM","")
                                             .str.replace("PM",""))
            data[self.di_columns["add"][0]] = (data[self.di_columns["time"]].str.extract(
                                               r"(\d{1,4}[/-]\d{1,2}[/-]\d{1,4})"))
            data[self.di_columns["add"][1]] = (data[self.di_columns["time"]].str.extract(
                                               r"(\d{1,2}[:]\d{1,2})"))

            for tipe, columns in self.di_columns["type_columns"].items():
                data =  self.ac_data.format_column(data, columns, tipe)

            df_data = data[self.di_columns["order_final"]]
            df_concat = pd.concat([df_concat, df_data], ignore_index=True)

            for key in self.di_replace["unify"]:
                df_concat[key] = (df_concat[key].replace(self.di_replace["unify"][key]))

        return df_concat

    def write_tables(self, df_data:pd.DataFrame, st_path:str, st_type:str)->None:
        """ DOC
        """
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


class GoldProcess:
    """DOC
    """

    def __init__(self):
        """DOC
        """
        self.st_path_ori = DI_SCOPE["slv"]["path"]
        self.st_path_fin = DI_SCOPE["gld"]["path"]
        self.di_order = {}

    def execute(self):
        """DOC
        """
        self.extract_data()
        df_data = self.denormalize()
        df_data = self.rules(df_data)
        self.write_tables(df_data, "sale_product.csv", "d")

    def extract_data(self):
        """DOC"""
        ls_files = os.listdir(self.st_path_ori)

        for file in ls_files:
            if "dim" in file or "fact" in file:
                st_name = file[:-4]
                st_path = self.st_path_ori / file
                df_data = pd.read_csv(st_path)
                self.di_order[st_name] = df_data

    def denormalize(self)->pd.DataFrame:
        """DOC"""

        name = 0
        data = 0

        for name, data in self.di_order.items():
            if "fact" in name:
                df_data = data
            else:
                st_name = name[4:]
                df_data = df_data.merge(data, on=f"id_{st_name}")

        df_data = df_data[["id_venta", "fecha", "hora", "puesto", "producto", "categoria", "canal",
                           "metodo_pago", "precio"]]

        return df_data

    def rules(self, df_data:pd.DataFrame)->pd.DataFrame:
        """DOC"""

        df_data = df_data.loc[df_data["precio"] > 0]

        return df_data

    def write_tables(self, df_data:pd.DataFrame, st_path:str, st_type:str)->None:
        """ DOC
        """
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

# Finite Incantatem
