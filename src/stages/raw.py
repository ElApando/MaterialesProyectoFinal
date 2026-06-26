"""DOCT"""

#pylint: disable=import-error

import os
from pathlib import Path

from villapy_lib.filesystem.files_utils import ManageFile # type:ignore
from villapy_lib.looging.write_log import WriteLogs # type:ignore

from config.static import DI_SCOPE

class RawProcess:
    """ Proceso RAW """

    def __init__(self)-> None:
        """ Proceso Raw

        Se encarga de extraer la información cruda de la ubicación en la que fue
        puesta por el cliente y la coloca en la ruta RAW para que el proyecto extraiga los datos
        """
        self.ac_folder: ManageFile = ManageFile()
        self.ac_logs: WriteLogs = WriteLogs()
        self.st_path_ori: Path = DI_SCOPE["ori"]["path"]
        self.st_path_raw: Path = DI_SCOPE["raw"]["path"]


    def execute(self)-> None:
        """ Ejecución
        
        Accede a la locación del cliente par extraer todos los archivos de la ubicación dada por el
        cliente, se copia y se pega en la carpeta RAW del mismo proyecto
        """
        self.ac_logs.write_logs("[INFO] - Extracción de datos")
        ls_files = os.listdir(self.st_path_ori)

        for file in ls_files:
            st_ori: Path = self.st_path_ori / file
            st_raw: Path = self.st_path_raw / file
            self.ac_folder.move_file(st_ori, st_raw, "copy")
            self.ac_logs.write_logs(f"[INFO] - Cambio de archivos a carpeta raw {file}")

# Fintie Incantatem
