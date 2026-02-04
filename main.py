"""Orquestador de Web Scarping Natura

Este es el contrrol de mando de todo el proyecto, se debe de colocar True o False dependiendo de lo
que se revisa

Se ejecuta con => python -m main
"""

# pylint: disable=broad-exception-caught

from typing import Callable

from config.static import DI_SCOPE
from src.flow.extract_data import RawProcess, BronzeProcess, SilverProcess, GoldProcess
from src.utils.tools import FolderManage

# ------------------------------------------------------------------------------------------
#  Control de Mando (Modificar de acuerdo a la necesidad)
# ------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------
# De este punto en adelante no se debe de tocar nada para aquellos que no son Programadores
# ------------------------------------------------------------------------------------------

def run_flow(st_decription:str, function: Callable, *args, **kwargs)->None:
    """ Ejecución del Flujo

    La función tiene como finalidad elimnar redundancia y encapsular las escrituras de los
    registros evitando la verborrea y dar oportunidad de documentar los errores de forma
    puntual

    Parameters:
        - st_description *(str)* - Descripción del proceso o paso actual del flujo.
        - function *(Callable)* - Función que se ejecutará dentro del flujo.
        - *args - Argumentos posicionales adicionales que se pasarán a la función.
        - **kwargs - Argumentos con nombre adicionales que se pasarán a la función.

    Returns:
        - None -

    """

    write_logs("general", f"--> Comienza - {st_decription}")

    try:
        function(*args, **kwargs)

    except Exception as e:
        write_logs("general", f"--> Fallo - {st_decription} -\n {e}")
        raise KeyError("Error") from e

    write_logs("general", f"--> Finaliza - {st_decription}")

def main()->None:
    """Orquestador

    El orquestador dirige tla ejecución de todo el flujo logrando obtener el procesamiento de datos
     de forma ordenada y correcta.
    La función exrtae, procesa y sube los datos provenientes de Mercado Libre y Ripley, además de
    que tambien transforma las tablas Bronce a Plata que son utilizadas en el proceso final el cual
     comprende de la desnormalización del nivel Plata generando la tabla que consume el cliente.

    Parameters:
        - None -
    Returns:
        - None -
    """


    # Revisión de rutas
    for _, route in DI_SCOPE.items():
        ac_folder = FolderManage()
        ac_folder.check_path(route["path"])

    ac_extract = RawProcess()
    # ac_extract.execute()
    ac_extract = BronzeProcess()
    ac_extract.execute()
    ac_extract = SilverProcess()
    ac_extract.execute()
    ac_extract = GoldProcess()
    ac_extract.execute()


    # write_logs("general", "\n")
    # write_logs("general", "%"*50)
    # write_logs("general", "Inicio orquestador")



    # # --- Natura ---
    # if di_configuration["Natura"]["extract"]:
    #     activate = ws_natura_general.Natura(ST_SEARCH)
    #     run_flow("Extracción Natura", activate.execute)
    # if di_configuration["Natura"]["upload"]:
    #     activate = upload_getallcompany.GetNatura("natura")
    #     run_flow("Carga Natura", activate.execute)

    # # --- Tabla Union ---
    # if di_configuration["manage_table"]["table_union"]:
    #     activate = data_manipulation.CleanDataFinal()
    #     run_flow("Creación de la tabla unión", activate.execute)

    # # --- Falabella ---
    # if di_configuration["Falabella"]["extract"]:
    #     activate = ws_falabella_general.Falabella(ST_SEARCH)
    #     run_flow("Extracción Falabella", activate.execute)
    # if di_configuration["Falabella"]["upload"]:
    #     activate = upload_getallcompany.GetAllCompany("falabella")
    #     run_flow("Carga Falabella", activate.execute)

    # # --- Mercado Libre ---
    # if di_configuration["MercadoLibre"]["extract"]:
    #     activate = ws_mercado_general.MercadoLibre(ST_SEARCH)
    #     run_flow("Extracción Mercado Libre", activate.execute)
    # if di_configuration["MercadoLibre"]["upload"]:
    #     activate = upload_getallcompany.GetAllCompany("mercado")
    #     run_flow("Carga Mercado", activate.execute)

    # # --- Ripley ---
    # if di_configuration["Ripley"]["extract"]:
    #     activate = ws_ripley_general.Ripley(ST_SEARCH)
    #     run_flow("Extracción Ripley", activate.execute)
    # if di_configuration["Ripley"]["upload"]:
    #     activate = upload_getallcompany.GetAllCompany("ripley")
    #     run_flow("Carga Ripley", activate.execute)

    # # --- Transformación Bronze->Silver ---
    # if di_configuration["manage_table"]["brz->slv"]["option"]:
    #     for ii in range(0, len(DI_SCOPE_TABLES["name_complete"]), 1):
    #         st_name = DI_SCOPE_TABLES["name_complete"][ii]
    #         activate = transform_brz_slv.SilverPerformance(
    #                     DI_SCOPE_TABLES["name_complete"][ii],
    #                     di_configuration["manage_table"]["brz->slv"]["medal"],
    #                     DI_SCOPE_TABLES["type"][ii],
    #                     di_configuration["manage_table"]["brz->slv"]["ejecution"])
    #         run_flow(f"Transformación Bronze->Silver {st_name}", activate.execute)

    # # --- Tabla Gold ---
    # if di_configuration["manage_table"]["table_mean"]:
    #     activate = group_data_finaly.ExtractAll("slv")
    #     run_flow("Creación de la tabla final", activate.execute)

    # # --- Visualización de datos ---
    # if di_configuration["view"]["all"]:
    #     run_flow("Visualización de datos", check_data_base)

    # write_logs("general", "Finaliza orquestador")
    # write_logs("general", "%"*50)

if __name__ == "__main__":
    # try:
    main()

    # except Exception as e:
        #write_logs("general", f"Algo salio terriblemente mal a continuación el error -\n {e}")

# Finite Incantatem
