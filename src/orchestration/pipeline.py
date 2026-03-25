"""DOC
"""

import time
from typing import Callable

from villapy.looging.write_log import WriteLogs
from villapy.utils.functions import ManageFunctions

from src.stages import raw, bronze, silver, gold

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

        self.ac_logs: WriteLogs = WriteLogs()
        self.cl_func: ManageFunctions = ManageFunctions()

        self.raw = raw.RawProcess()
        self.bronze = bronze.BronzeProcess()
        self.silver = silver.SilverProcess()
        self.gold = gold.GoldProcess()

    def excute(self)->None:
        """Ejecución
        
        La función ejecuta los diferentes procesos del pipeline ETL"""
        self.ac_logs.write_logs("\n")
        self.ac_logs.write_logs("[START] - Ejecución Pipeline ")
        self.cl_func.run_stage("raw", self.raw.execute)
        # self.ac_logs.write_logs("\n")
        # self._run_stage("brz", self.bronze.execute)
        # self.ac_logs.write_logs("\n")
        # self._run_stage("slv", self.silver.execute)
        # self.ac_logs.write_logs("\n")
        # self._run_stage("gld", self.gold.execute)

        self.ac_logs.write_logs("[END] - Ejecución Pipeline ")

    def _run_stage(self, st_name_process: str, fu_function: Callable[..., None])->None:
        """ Run Stage
        
        La función registra la actividad de las funciones que se ejecutan. Registra tiempo de
        ejecución, que proceso se ejecuta, y si hubo algun error en la ejecución

        Parameters:
            st_name_process (str): Nombre del proceso que se ejecutará
            fu_function (Callable): Función en cuestión
        """
        fl_start_time = time.time()
        self.ac_logs.write_logs(f"[START] - Proceso {st_name_process}")

        try:
            fu_function()
            fl_duration = time.time() - fl_start_time
            self.ac_logs.write_logs(
                            f"[INFO] - Tiempo de ejecución {st_name_process} - {fl_duration:.2f}s")
            self.ac_logs.write_logs(f"[END] - Proceso {st_name_process}")

        except Exception as e:
            self.ac_logs.write_logs(f"[FALLO] - {st_name_process} - Error {e}")
            raise

# Finite Incantatem
