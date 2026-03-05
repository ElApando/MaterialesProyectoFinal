"""Configuración dínamica del proyecto

Se configuara el ambiente en el que se este trabajando. los ambientes con los que trabaja el
proyecto son: Local y Google Colab, por el momento, proximamente tambien se incluira docker 
"""

from pathlib import Path

ST_FLOW  = "content"

ST_CURRENT_PATH = Path.cwd()

if ST_FLOW in str(ST_CURRENT_PATH):
    st_path_base = Path("/content/drive/MyDrive")
    st_path_orig = st_path_base / "Proyecto Rutilio" / "Datos Crudos"
    st_path_base = st_path_base / "Proyecto Rutilio"

else:
    st_path_base = ST_CURRENT_PATH
    st_path_orig = ST_CURRENT_PATH / "MaterialesProyectoFinal"

# Finite incantatem
