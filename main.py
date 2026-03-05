"""Orquestador de Proyecto - Rutilio

Este es el comienzo del proyecto, se crean las carpetas de uso si no existen y posteriormente
se activa el Pipeline.

Se ejecuta con => python -m main
"""

from config.static import DI_SCOPE
from src.orchestration.pipeline import Pipeline
from src.utils.tools import FolderManage, write_logs

def main()->None:
    """
    Ejecuta la función principal del proyecto.

    Crea las carpetas necesarias y posteriormente ejecuta el pipeline.
    """

    write_logs("\n")
    write_logs("()"*30)
    write_logs("[START] - main")
    write_logs("\n")

    for _, route in DI_SCOPE.items():
        ac_folder = FolderManage()
        ac_folder.check_path(route["path"])
        st_path = route["path"]
        write_logs(f"[INFO] - Crecaión de la ruta {st_path}")

    activate = Pipeline()
    activate.excute()

    write_logs("\n")
    write_logs("[END] - main")
    write_logs("()"*30)

if __name__ == "__main__":
    main()

# Finite Incantatem
