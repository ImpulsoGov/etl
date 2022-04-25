from __future__ import annotations
from typing import Final
import requests
import urllib
import pandas as pd
from io import StringIO
from impulsoetl.tipos import DatetimeLike
from impulsoetl.sisab.parametros_requisicao import head

VISOES_EQUIPE_CODIGOS: Final[dict[str, str]] = {
    "todas-equipes": "",
    "equipes-homologadas": "|HM|",
    "equipes-validas": "|HM|NC|AQ|",
}

NIVEL_AGREGACAO_CODIGOS: Final[dict[str, str]] = {
    "municipios": "ibge",
    "estabelecimentos_equipes": "cnes_ine"
}
def _extrair_parametros(
    visao_equipe: str,
    competencia: DatetimeLike,
    nivel_agregacao: str
) -> str:
    competencia = competencia.replace('-','')
    competencia = competencia[0:6]
    url = (
        "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal"
        + "/indicadores/indicadorCadastro.xhtml"
    )
    hd = head(url)
    vs = hd[1]
    ponderacao = ''
    visao_equipe_codigos = urllib.parse.quote(VISOES_EQUIPE_CODIGOS[visao_equipe])
    headers = hd[0]
    payload = (
        "j_idt44=j_idt44&selectLinha="
        + NIVEL_AGREGACAO_CODIGOS[nivel_agregacao]
        + "&opacao-capitacao="
        + visao_equipe_codigos
        + ponderacao
        + "&competencia="
        + competencia
        + "&javax.faces.ViewState="
        + vs
        + "&j_idt83=j_idt83"
    )
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text

def extrair_parametros(
    visao_equipe: str,
    competencia: DatetimeLike,
    nivel_agregacao: str
) -> pd.DataFrame:

    resposta = _extrair_parametros(
        visao_equipe=visao_equipe,
        competencia=competencia,
        nivel_agregacao=nivel_agregacao
    )

    df = pd.read_csv(StringIO(resposta), delimiter='\t', header=None, engine= 'python')
    dados = df.iloc[9:-4]
    df = pd.DataFrame(data=dados)
    df=df[0].str.split(';', expand=True)
    if nivel_agregacao == 'municipios':
        df.columns=['Uf','IBGE','Municipio','quantidade','parametro','coluna'] 
    else:
        df.columns=['Uf','IBGE','Municipio','CNES','Nome UBS','INE','Sigla','quantidade','parametro','Coluna']
    return df


