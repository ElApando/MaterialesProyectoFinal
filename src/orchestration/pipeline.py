"""DOC
"""

import time
from typing import Callable

from src.stages import raw, bronze, silver, gold
from src.utils.tools import write_logs

class Pipeline:
    """Pipeline"""

    def __init__(self)->None:
        """Pipeline
        
        La clase contien el Pipeline ETL, el cual utiliza la arquitectura medallon por lo que se
        utiliza: 
        
        - Raw - Extre la información de la fuente cruda y la colca en Raw, 
        - Bronze - Toma la data de Raw y revisa que la estructura sea correcta,
        - Silver - De Bronze toma la estructura y revisa el contenido de la data, además de
        verificar la integridad referencial de las diferentes tablas,
        - Gold - Se desnormaliza la data obtenida con la finalidad de presentarla en BigQuery        
        """
        self.raw = raw.RawProcess()
        self.bronze = bronze.BronzeProcess()
        self.silver = silver.SilverProcess()
        self.gold = gold.GoldProcess()

    def excute(self)->None:
        """Ejecución
        
        La función ejecuta los diferentes procesos del pipeline ETL"""
        write_logs("\n")
        write_logs("[START] - Ejecución Pipeline ")
        self._run_stage("raw", self.raw.execute)
        write_logs("\n")
        self._run_stage("brz", self.bronze.execute)
        write_logs("\n")
        self._run_stage("slv", self.silver.execute)
        write_logs("\n")
        self._run_stage("gld", self.gold.execute)

        write_logs("[END] - Ejecución Pipeline ")

    def _run_stage(self, st_name_process: str, fu_function: Callable[..., None])->None:
        """ Run Stage
        
        La función registra la actividad de las funciones que se ejecutan. Registra tiempo de
        ejecución, que proceso se ejecuta, y si hubo algun error en la ejecución

        Parameters:
            st_name_process (str): Nombre del proceso que se ejecutará
            fu_function (Callable): Función en cuestión
        """
        fl_start_time = time.time()
        write_logs(f"[START] - Proceso {st_name_process}")

        try:
            fu_function()
            fl_duration = time.time() - fl_start_time
            write_logs(f"[INFO] - Tiempo de ejecución {st_name_process} - {fl_duration:.2f}s")
            write_logs(f"[END] - Proceso {st_name_process}")

        except Exception as e:
            write_logs(f"[FALLO] - {st_name_process} - Error {e}")
            raise

# Finite Incantatem
