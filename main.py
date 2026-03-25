"""Orquestador de Proyecto - Rutilio

Este es el comienzo del proyecto, se crean las carpetas de uso si no existen y posteriormente
se activa el Pipeline.

Se ejecuta con => python -m main
pip install -e "C:/Users/DELL/Respaldo/DON VILLA NUEVO/UNIVERSITY/Proyecto/PYTHON/GitHub/villapy"
"""

import villapy.looging.write_log as wl
from villapy.looging.write_log import WriteLogs
from villapy.filesystem.files_utils import ManageFile

from config.static import DI_SCOPE
from src.orchestration.pipeline import Pipeline
print(dir(wl))
def main()->None:
    """
    Ejecuta la función principal del proyecto.

    Crea las carpetas necesarias y posteriormente ejecuta el pipeline.
    """

    ac_logs: WriteLogs = WriteLogs()
    print(dir(ac_logs))

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
