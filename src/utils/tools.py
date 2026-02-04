"""
Docstring for src.utils.tools
"""

import os
import re
import shutil
from pathlib import Path

import sqlite3
import unicodedata
import pandas as pd


class FolderManage:
    """ DOC
    """

    def __init__(self)->None:
        """
        Docstring for __init__
        
        :param self: Description
        :param st_path: Description
        :type st_path: str
        """

    def check_path(self, st_path:str)->None:
        """
        Docstring for check_path
        
        :param self: Description
        """

        if not self._validate_path(st_path):
            raise ValueError("Ruta Invalida")

        if not os.path.exists(st_path):
            os.makedirs(st_path, exist_ok=True)

    def move_file(self, st_path_origing:str, st_path_final:str, st_type:str)->None:
        """ DOC
        """
        st_type = st_type.upper()
        st_path_origing = Path(st_path_origing)
        st_path_final = Path(st_path_final)

        if st_type == "CUT":
            st_path_origing.rename(st_path_final)

        elif st_type == "COPY":
            shutil.copy2(st_path_origing, st_path_final)

        else:
            raise KeyError("Esa Opción no existe en el método")

    def _validate_path(self, st_path:str)->bool:
        """
        Docstring for _validate_path
        
        :param self: Description
        :param st_path: Description
        :type st_path: str
        :return: Description
        :rtype: bool
        """
        try:
            Path(st_path).resolve()
            return True
        except Exception as e:
            # log
            print(e)
            return False


class DataManage:
    """DOC"""

    def __int__(self):
        """DOC"""
        pass

    def separator_table(self, st_path:str)->str:
        """DOC
        """
        st_path = Path(st_path)

        with st_path.open("r", encoding = "utf-8") as file:
            content = file.readline()

        st_separator = self.separator_what(content)

        return st_separator

    def extract_tables_sqlite(self, st_path:str)->dict:
        """DOC"""

        di_tables = {}

        conection = sqlite3.connect(st_path)
        ls_tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';",
                            conection)["name"].tolist()

        for table in ls_tables:
            df_data = pd.read_sql_query(f"SELECT * FROM {table}",conection)
            di_tables[table] = df_data

        conection.close()

        return di_tables

    def separator_what(self, st_word:str)->str:
        """ DOC
        """

        if "|" in st_word:
            st_separator = "|"

        elif ";" in st_word:
            st_separator = ";"

        elif "\t" in st_word:
            st_separator = "\t"

        elif "," in st_word:
            st_separator = ","

        else:
            raise ValueError("Separador no encontrado")

        return st_separator

    def clean_filename(self, st_file_name)->str:
        """Limpia el nombre de carcteres etraños que puedan romper el nombre de la tabla
        
        Parameters:
            - st_file_name *(str)* - Nombre con caracteres raros
        Returns:
            - st_file_name *(str)* - Nombre sin caracteres raros
        """
        st_file_name = re.sub(r'[<>:"/\\|?*]',"", st_file_name)
        return st_file_name

    def clean_filename_accent(self, st_text)->str:
        """Liempieza de los acentos de las palabras

        Parameters:
            - st_text *(str)* - Nombre con acentos
        Returns:
            - st_text *(str)* - Nombre sin acentos
        """
        return ''.join(
            c for c in unicodedata.normalize('NFKD', st_text)
            if not unicodedata.combining(c)
        )

    def save_numbers(self, st_number)->str:
        """ Salvación de números

        """
        st_number = str(st_number)
        ls_pattern = re.findall(r"\d+\.\d+", st_number)
        if ls_pattern:
            st_number = re.findall(r"\d+\.\d+", st_number)[0]

        return st_number

    def modify_date(self, st_date):
        """DOC
        """

        di_months = {"jan":"01", "ene":"01", "feb": "02", "mar": "03", "apr": "04", "abr":"04",
                    "may": "05", "jun": "06", "jul":"07", "aug": "08", "ago": "08" , "sep": "09",
                    "oct": "10", "nov": "11", "dic":"12", "dec":"12"}

        ls_one = st_date.split("/")

        if len(ls_one) == 1:
            ls_one = st_date.split("-")

        if ls_one[1].isalpha():
            ls_one[1] = di_months[(ls_one[1]).lower()]
            st_save = ls_one[0]
            ls_one[0] = ls_one[2]
            ls_one[2] = st_save

            if len(ls_one[0]) == 2:
                ls_one[0] = f"20{ls_one[0]}"

        if len(ls_one[2]) == 4:
            st_save = ls_one[0]
            ls_one[0] = ls_one[2]
            ls_one[2] = st_save

        if int(ls_one[1]) > 12:
            st_save = ls_one[2]
            ls_one[2] = ls_one[1]
            ls_one[1] = st_save

        st_date = "-".join(ls_one)

        return st_date

    def format_column(self, df_data:pd.DataFrame, ls_names:list[str], st_type_data:str)->pd.DataFrame:
        """ Convierte el tipo de dato de columnas específicas de un DataFrame.

        Parameters:
            - df_data *(pd.DataFrame)* - DataFrame original.
            - ls_names *(list)* - Lista de columnas a convertir.
            - st_type_data *(str)* - Tipo de dato destino ("str", "int", "float", "date").

        Retorna:
            - df_data *(pd.DataFrame)* - Nuevo DataFrame con las columnas convertidas.
        """

        for col in ls_names:
            if not col in df_data.columns:
                raise KeyError("La columna ingresada no está contenida en el DataFrame!")

        if st_type_data == "str":
            for col in ls_names:
                df_data[col] = df_data[col].astype(str).replace("nan",None)
                df_data[col] = df_data[col].apply(lambda word: word.strip())
                df_data[col] = df_data[col].str.upper()
                df_data[col] = df_data[col].apply(self.clean_filename)
                df_data[col] = df_data[col].apply(self.clean_filename_accent)

        elif st_type_data == "int":
            for col in ls_names:
                df_data[col] = pd.to_numeric(df_data[col], errors='coerce').fillna(0).astype(int)

        elif st_type_data == "float":
            for col in ls_names:
                df_data[col] = df_data[col].apply(self.save_numbers)
                df_data[col] = pd.to_numeric(df_data[col], errors='coerce').fillna(0).astype(float)

        elif st_type_data == "date":
            for col in ls_names:
                df_data[col] =  df_data[col].apply(self.modify_date)
                df_data[col] = pd.to_datetime(df_data[col], errors='coerce')

        elif st_type_data == "time":
            for col in ls_names:
                df_data[col] = pd.to_datetime(df_data[col], format = "%H:%M").dt.time

        else:
            raise KeyError("Esa opcion no existe!")

        return df_data


class ValidateData:
    """
    Docstring for ValidateData
    """""

    def __init__(self, st_kind:str)->None:
        self.st_kind = st_kind

    def string_check(self, st_word:str)->str:
        """
        Docstring for string_check
        
        :param self: Description
        :param st_word: Description
        :type st_word: str
        """
        if not self.filter_caracters(st_word):
            raise ValueError(f"La {self.st_kind} es invalida")

        return st_word

    def filter_caracters(self, st_word:str)->bool:
        """DOC
        """
        st_pattern = r'[<>:"|?*]'

        return not re.search(st_pattern, st_word)

def write_logs(st_name_rate: str, st_text: str)->None:
    """Escribe lols registros del proceso en cuestión

    Parameters:
        - st_name_rate *(str)* - nombre del proceso
        - st_text *(str)* - Mensaje que se guarda en los registros
    Returns:
        - None
    """
    ls_registry =os.listdir(f"{dynamic.ST_ROUTE_ORIGIN}logs/")
    st_name = st_name_rate.lower()

    if f"{dynamic.ST_ROUTE_ORIGIN}logs_{st_name}.txt" in ls_registry:
        with open(f"{dynamic.ST_ROUTE_ORIGIN}logs/logs_{st_name}.txt","a",encoding="utf-8") as file:
            st_time = str(datetime.datetime.now())
            file.write(f"\n{st_time} {st_text}")
    else:
        with open(f"{dynamic.ST_ROUTE_ORIGIN}logs/logs_{st_name}.txt","a",encoding="utf-8") as file:
            st_time = str(datetime.datetime.now())
            file.write(f"\n{st_time} {st_text}")