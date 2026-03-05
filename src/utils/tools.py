""" Herramientas de Programación
"""

import os
import re
import shutil
from typing import Dict, List
from pathlib import Path
import datetime

import sqlite3
import unicodedata
import pandas as pd

from config import dynamic

class FolderManage:
    """ Manejo de Carpetas """

    def __init__(self)->None:
        """ Manejo de Carpetas 
        
        La clase considera:
            - check_path : Revisión de que la ruta sea valida para el proyecto
            - move_file : Movimiento de archvios, ya sea copiando y pegando o cortando y pegando
        """

    def check_path(self, pa_path:Path)->None:
        """ Revisión de rutas

        Revisa que la ruta ingresada sea valida en los diferentes sistemas operativos, además de
         crear la ruta si no existe

        Parameters:
            pa_path (Path): Ruta que será revisada
        """

        if not self._validate_path(pa_path):
            raise ValueError("Ruta Invalida")

        if not os.path.exists(pa_path):
            os.makedirs(pa_path, exist_ok=True)

    def move_file(self, pa_path_origing:Path, pa_path_final:Path, st_type:str)->None:
        """ Movimiento de archivos

        Copia o Corta el archivo del destino de origen y lo pega en la carpeta correspondiente

        Parameters:
            pa_path_origing (Path): Ruta de origen del archivo
            pa_path_final (Path): Ruta en la que se colocara el archivo de interes
            st_type (str): Tipo de movimiento que se realizará, ya sea [CUT, COPY]  
        """
        st_type = st_type.upper()
        pa_path_origing = Path(pa_path_origing)
        pa_path_final = Path(pa_path_final)

        if st_type == "CUT":
            pa_path_origing.rename(pa_path_final)

        elif st_type == "COPY":
            shutil.copy2(pa_path_origing, pa_path_final)

        else:
            raise KeyError("Esa Opción no existe en el método")

    def _validate_path(self, pa_path:Path)->bool:
        """  _validate_path

        Revisión de que el Path es valido
        
        Parameters:
            pa_path (Path): Ruta qeu será evaluada 
        
        Return:
            bool: Retorna un booleano indicando si es valida la ruta o no
        """
        try:
            Path(pa_path).resolve()
            return True

        except Exception as e:
            write_logs(f"{e}")
            return False


class DataManage:
    """ Manejo de datos """

    def __int__(self):
        """ Manejo de datos 
        
        La clase maneja datos mediante las funciones:

        - separator_table: Se obtiene el separador de los archivos CSV y TXT
        - extract_tables_sqlite: Se obtienen los tados provenientes de la base de datos
        - seperator_what: Busca que separador tienen los datos en los archivos de CSV y TXT
        """

    def separator_table(self, pa_path:Path)->str:
        """ separator_table

        Se obtiene el separador de los archivos CSV y TXT, dado que en ocasiones los archivos
        se encuentran sucios.

        Parameters:
            pa_path (Path): Ruta del archivo

        Returns:
            st_separator (str): Separador de datos, necesario para hacer la extracción de datos
            con pandas
        """
        st_path = Path(pa_path)

        with st_path.open("r", encoding = "utf-8") as file:
            content = file.readline()

        st_separator = self.separator_what(content)

        return st_separator

    def extract_tables_sqlite(self, st_path:Path)->Dict[str, pd.DataFrame]:
        """ extract_tables_sqlite
        
        Parameters:
            pa_path (Path): Ruta del archivo

        Returns:
            di_tables (Dict): Diccionario con los datos de las tablas SQL
        """
        di_tables: Dict[str, pd.DataFrame] = {}

        conection = sqlite3.connect(st_path)
        ls_tables: List[str] = pd.read_sql(  # type: ignore
        "SELECT name FROM sqlite_master WHERE type='table';", conection)["name"].tolist()

        for table in ls_tables:
            df_data = pd.read_sql_query(f"SELECT * FROM {table}",conection) # type: ignore
            di_tables[table] = df_data

        conection.close()

        return di_tables

    def separator_what(self, st_word:str)->str:
        """ separator_what

        Los archivos en ocaciónes se encuentran suciós por lo que se busca es que tipo de separador
        tiene el archivo.

        Parameters:
            st_word (str): Cadena de texto que se revisa
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

    def clean_filename(self, st_file_name:str)->str:
        """Limpia el nombre de carcteres etraños que puedan romper el nombre de la tabla
        
        Parameters:
            st_file_name *(str)*: Nombre con caracteres raros
        Returns:
            st_file_name *(str)*: Nombre sin caracteres raros
        """
        st_file_name = re.sub(r'[<>:"/\\|?*]',"", st_file_name)
        return st_file_name

    def clean_filename_accent(self, st_text:str)->str:
        """Liempieza de los acentos de las palabras

        Parameters:
            st_text *(str)*: Nombre con acentos
        Returns:
            st_text *(str)*: Nombre sin acentos
        """
        return ''.join(
            c for c in unicodedata.normalize('NFKD', st_text)
            if not unicodedata.combining(c)
        )

    def save_numbers(self, st_number:str)->str:
        """ Salvación de números

        Se recatan los numeros de las fechas que se encuentran mal formados

        Parameters:
            st_number (str): Cadena de texto a evaluar
        
        Returns:
            st_number (str): Número encontrado
        """
        st_number = str(st_number)
        ls_pattern = re.findall(r"\d+\.\d+", st_number)

        if ls_pattern:
            st_number = re.findall(r"\d+\.\d+", st_number)[0]

        return st_number

    def modify_date(self, st_date:str):
        """ Modificación de fecha

        Se da formato a las fechas mal formadas 

        Parameters:
            st_data (str): Fecha con formato incierto

        Return:
            st_data (str): Fecha con el formato adecuado 
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

    def format_column(self, df_data:pd.DataFrame, ls_names:list[str],
                      st_type_data:str)->pd.DataFrame:
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
                df_data[col] = df_data[col].astype(str).replace("nan",None) # type:ignore
                df_data[col] = df_data[col].apply(lambda word: word.strip()) # type:ignore
                df_data[col] = df_data[col].str.upper()
                df_data[col] = df_data[col].apply(self.clean_filename) # type:ignore
                df_data[col] = df_data[col].apply(self.clean_filename_accent) # type:ignore

        elif st_type_data == "int":
            for col in ls_names:
                df_data[col] = (pd.to_numeric(df_data[col], errors='coerce') # type:ignore
                                .fillna(0).astype(int))

        elif st_type_data == "float":
            for col in ls_names:
                df_data[col] = df_data[col].apply(self.save_numbers) # type:ignore
                df_data[col] = (pd.to_numeric(df_data[col], errors='coerce') # type:ignore
                                .fillna(0).astype(float))

        elif st_type_data == "date":
            for col in ls_names:
                df_data[col] =  df_data[col].apply(self.modify_date) # type:ignore
                df_data[col] = pd.to_datetime(df_data[col], errors='coerce')

        elif st_type_data == "time":
            for col in ls_names:
                df_data[col] = pd.to_datetime(df_data[col], format = "%H:%M").dt.time

        else:
            raise KeyError("Esa opcion no existe!")

        return df_data


class ValidateData:
    """ Validación de datos """

    def __init__(self)->None:
        """ Validación de datos

        Validación de datos de tipo cadena de texto, la clase considera 

        - string_check : revisión de que la cadena de texto no contenga caracteres extraños
        - filter_caracters : Limpieza de caracteres extraños en la cadena de texto
        """
        return None

    def string_check(self, st_word:str)->str:
        """ String_Check

        Revisión de que la palabra sea una cadena de texto

        Parameteres:
            st_word (str): Palabra de evaluación

        Returns:
            st_word (str): Palabra evaluada
        """
        if not self.filter_caracters(st_word):
            raise ValueError(f"La {st_word} es invalida")

        return st_word

    def filter_caracters(self, st_word:str)->bool:
        """ Filtro de caracteres

        Parameters:
            st_word (str): Palabra que será limpiado
        
        Returns:
            st_word (str): Palabra limpia de los caracteres especiales
        """
        st_pattern = r'[<>:"|?*]'

        return not re.search(st_pattern, st_word)

def write_logs(st_text: str)->None:
    """ Escribe lols registros del proceso en cuestión

    Parameters:
        st_text (str): Mensaje que se guarda en los registros
    """

    with open(f"{dynamic.st_path_base}/logs/logs_{datetime.datetime.now().date()}.txt", "a",
              encoding="utf-8") as file:
        st_time = str(datetime.datetime.now())
        file.write(f"\n{st_time} {st_text}")

# Finite Incantatem
