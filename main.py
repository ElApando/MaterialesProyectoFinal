"""Orquestador de Web Scarping Natura

Este es el contrrol de mando de todo el proyecto, se debe de colocar True o False dependiendo de lo
que se revisa

Se ejecuta con => python -m main
"""

# pylint: disable=broad-exception-caught

from config.static import DI_SCOPE
from src.flow.extract_data import RawProcess, BronzeProcess, SilverProcess, GoldProcess
from src.utils.tools import FolderManage

def main()->None:
    """DOC-
    """


    # Revisión de rutas
    for _, route in DI_SCOPE.items():
        ac_folder = FolderManage()
        ac_folder.check_path(route["path"])

    ac_extract = RawProcess()
    ac_extract.execute()
    ac_extract = BronzeProcess()
    ac_extract.execute()
    ac_extract = SilverProcess()
    ac_extract.execute()
    ac_extract = GoldProcess()
    ac_extract.execute()

if __name__ == "__main__":
    main()

# Finite Incantatem
