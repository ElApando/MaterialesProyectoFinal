"""
Docstring for config.static

python -m config.static
"""
from typing import Dict, Any
from config.dynamic import st_path_base, st_path_orig

DI_SCOPE : Dict[str, Any] = {"ori":{"path": st_path_orig},
                             "raw":{"path": st_path_base / "refs" / "raw"},
                             "brz":{"path": st_path_base / "refs" / "brz"},
                             "slv":{"path": st_path_base / "refs" / "slv"},
                             "gld":{"path": st_path_base / "refs" / "gld"}}

DI_COLUMNS : Dict[str,Any] = {"id" : "id_venta",
                              "time" : "fecha_hora",
                              "add" : ["fecha", "hora"],
                              "type_columns": {
                                    "str":["id_venta", "fecha_hora", "puesto", "producto", 
                                           "categoria", "canal", "metodo_pago"],
                                    "float": ["precio"],
                                    "date": ["fecha"],
                                    "time": ["hora"]},
                              "order_final":["id_venta", "fecha", "hora", "puesto", "producto",
                                             "categoria","canal", "metodo_pago","precio"]}

DI_DIMENSION : Dict[str,Any] = {"dimension":
                                ["puesto", "producto", "categoria", "canal", "metodo_pago"]}

DI_REPLACE : Dict[str, Any] = {
                                "unify": {
                                    "producto": {""
                                    "VERDECITO" : "TAMAL VERDE",
                                    "VERDE" : "TAMAL VERDE",
                                    "ROJITO" : "TAMAL ROJO"}}}

# Finite Incantatem
