"""DOC
"""

# pylint: disable=import-error

from villapy_lib.looging.write_log import WriteLogs #type:ignore
from villapy_lib.utils.functions import ManageFunctions #type:ignore

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
        self.ac_logs.write_logs("\n")
        self.cl_func.run_stage("brz", self.bronze.execute)
        self.ac_logs.write_logs("\n")
        self.cl_func.run_stage("slv", self.silver.execute)
        self.ac_logs.write_logs("\n")
        self.cl_func.run_stage("gld", self.gold.execute)
        self.ac_logs.write_logs("[END] - Ejecución Pipeline ")

# Finite Incantatem
