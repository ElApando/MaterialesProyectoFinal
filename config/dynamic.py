"""
Docstring for config.dynamic
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
