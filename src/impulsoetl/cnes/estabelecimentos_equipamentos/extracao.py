import warnings

warnings.filterwarnings("ignore")
import json
import sys

import pandas as pd
import requests

sys.path.append(r"C:\Users\maira\Impulso\etl\src\impulsoetl")
from cnes.extracao_lista_cnes import extrair_lista_cnes

# from impulsoetl.loggers import logger


def extrair_equipamentos_estabelecimentos(
    codigo_municipio: str, lista_cnes: list
) -> pd.DataFrame:

    df_equipamentos = pd.DataFrame()

    for l in lista_cnes:

        try:

            url = (
                "http://cnes.datasus.gov.br/services/estabelecimentos-equipamento/"
                + coMun
                + l
            )

            payload = {}
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "Connection": "keep-alive",
                "Referer": "http://cnes.datasus.gov.br/pages/estabelecimentos/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
            }

            response = requests.request(
                "GET", url, headers=headers, data=payload
            )
            res = response.text

            parsed = json.loads(res)
            df = pd.DataFrame(parsed)
            df["municipio_id_sus"] = coMun
            df["estabelecimento_cnes_id"] = l
            df_equipamentos = df_equipamentos.append(df)

        except:
            pass

    return df_equipamentos

    # logger.info("Extração concluída")


coMun = "120001"
lista_codigos = extrair_lista_cnes(coMun)
equipamentos = extrair_equipamentos_estabelecimentos(coMun, lista_codigos)
data = equipamentos[
    [
        "municipio_id_sus",
        "estabelecimento_cnes_id",
        "dsTpEquip",
        "qtExiste",
        "qtUso",
        "tpSus",
        "dsEquipamento",
    ]
]
teste = data.loc[data["estabelecimento_cnes_id"] == "5701929"]
print(teste)
