"""Orquestador de Proyecto - Rutilio

Este es el comienzo del proyecto, se crean las carpetas de uso si no existen y posteriormente
se activa el Pipeline.

Se ejecuta con => python -m main
"""

# pylint: disable=import-error
 
from villapy_lib.looging.write_log import WriteLogs # type:ignore
from villapy_lib.filesystem.files_utils import ManageFile # type:ignore

from config.static import DI_SCOPE
from src.orchestration.pipeline import Pipeline

def main()->None:
    """
    Ejecuta la función principal del proyecto.

    Crea las carpetas necesarias y posteriormente ejecuta el pipeline.
    """

    ac_logs: WriteLogs = WriteLogs()

    ac_logs.write_logs("\n")
    ac_logs.write_logs("()"*30)
    ac_logs.write_logs("[START] - main")
    ac_logs.write_logs("\n")

    for _, route in DI_SCOPE.items():
        ac_folder = ManageFile()
        ac_folder.check_path(route["path"])
        st_path = route["path"]
        ac_logs.write_logs(f"[INFO] - Crecaión de la ruta {st_path}")

    activate = Pipeline()
    activate.excute()

    ac_logs.write_logs("\n")
    ac_logs.write_logs("[END] - main")
    ac_logs.write_logs("()"*30)

if __name__ == "__main__":
    main()

# Finite Incantatem
