from __future__ import annotations
from typing import Final
import requests
import pandas as pd
from io import StringIO
from datetime import date
from typing import TypedDict, Text
import numpy as np

# listar todos os municípios
MUNICIPIOS: Final[dict[str, str]] = {
    "Três Marias": "316935",
    }

# listar todos os estados por município
ESTADOS: Final[dict[str, str]] = {
    "Três Marias": "31",
}

# listar profissionais e códigos
PROFISSIONAIS: Final[dict[str, str]] = {
    "Enfermeiro" : "10",
    "Médico" : "14",
    "Nutricionista": "16",
    "Fisioterapeuta":"12"
    }

TIPO_ATENDIMENTO: Final[dict[str, str]] = {
    "Cons. agen. prog/cuid. cont." : "1",
    "Consulta agendada": "2",
    "Dem. esp. esc. inicial/orient." : "4",
    "Dem. esp. consulta no dia": "5",
    "Dem. esp. atendimento urgência": "6"
    }

# listar todos os periodos que devem ser extraídos
PERIODOS = ['202201','202202','202203','202204','202205','202206','202207','202208','202209']


COLUNAS_RENOMEAR: Final[dict[str, str]] = {
    "Probl/ Condição Avaliada": "problema_condicao_avaliada",
}

# colocar todas as colunas
TIPOS: Final[dict[str, str]] = {
    "problema_condicao_avaliada":str,
    "conduta":str,
    "municipio_id_sus":str,
    "municipio_uf":str,
    "periodo_data":str,
    "quantidade": int,
    "categoria_profissional":str,
    "tipo_atendimento": str,
}

class ParametrosRequisicao(TypedDict):
    estado_codigo: str
    municipio_codigo: str
    periodo_competencia: str
    categoria_profissional_codigo: str
    atendimento_tipo_codigo: str
    url: str

def realizar_requisicao(
    parametros: ParametrosRequisicao

):
    payload=(
            "j_idt44=j_idt44&lsCid="
            +"&dtBasicExample_length=10"
            +"&lsSigtap=&"
            +"td-ls-sigtap_length=10"
            +"&unidGeo=municipio&"
            +"estadoMunicipio="+parametros["estado_codigo"]
            +"&municipios="+parametros["municipio_codigo"]
            +"&j_idt76="+parametros["periodo_competencia"]
            +"&selectLinha=PCA"
            +"&selectcoluna=CDT"
            +"&j_idt89=eq-esf&j_idt89=eq-eacs&j_idt89=eq-nasf&j_idt89=eq-eab&j_idt89=eq-ecr&j_idt89=eq-sb&j_idt89=eq-epen&j_idt89=eq-eap"
            +"&categoriaProfissional="+parametros["categoria_profissional_codigo"]
            +"&idadeInicio=0"
            +"&idadeFim=0"
            +"&tpIdade="
            +"&tpProducao=4"
            +"&tipoAtendimento="+parametros["atendimento_tipo_codigo"]
            +"&condicaoAvaliada=ABP009&condicaoAvaliada=ABP008&condicaoAvaliada=ABP006&condicaoAvaliada=ABP010&condicaoAvaliada=ABP005&condicaoAvaliada=ABP007&condicaoAvaliada=ABP001&condicaoAvaliada=ABP004&condicaoAvaliada=ABP002&condicaoAvaliada=ABP003&condicaoAvaliada=ABP011&condicaoAvaliada=ABP012&condicaoAvaliada=ABP013&condicaoAvaliada=ABP014&condicaoAvaliada=ABP015&condicaoAvaliada=ABP019&condicaoAvaliada=ABP020&condicaoAvaliada=ABP018&condicaoAvaliada=ABP017&condicaoAvaliada=ABP023&condicaoAvaliada=ABP022&condicaoAvaliada=ABP024"
            +"&conduta=3&conduta=12&conduta=9&conduta=11&conduta=10&conduta=5&conduta=6&conduta=8&conduta=4&conduta=7&conduta=2&conduta=1"
            +"&javax.faces.ViewState=-7613125643820138900%3A-3026747708985996776&j_idt192=j_idt192"
            +"&j_idt192=j_idt192"
            )
    headers = {
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
  'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
  'Cache-Control': 'max-age=0',
  'Connection': 'keep-alive',
  'Content-Type': 'application/x-www-form-urlencoded',
  'Cookie': '_ga_M5P7XSD327=GS1.1.1664306143.2.0.1664306143.0.0.0; _ga_9F7EXER3ZL=GS1.1.1664306146.2.0.1664306146.0.0.0; _ga_1W5FB5P4BD=GS1.1.1664393865.4.0.1664393867.0.0.0; _ga=GA1.3.2078485526.1663857639; BIGipServerpool_sisab_jboss=!7DXWhpbyqGjvh72i4dOS8fa1J/wqqUMzR3mM7jNKx1xfXBNKQuynFHPO/YSB5BSKBTzzOX4I0i7C+Ao=; JSESSIONID=hCnRLLMQNYyssk0DMNz7pki1; _gid=GA1.3.807667890.1669140400',
  'Origin': 'https://sisab.saude.gov.br',
  'Referer': 'https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/saude/RelSauProducao.xhtml',
  'Sec-Fetch-Dest': 'document',
  'Sec-Fetch-Mode': 'navigate',
  'Sec-Fetch-Site': 'same-origin',
  'Sec-Fetch-User': '?1',
  'Upgrade-Insecure-Requests': '1',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
  'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"'
}

    resposta = requests.request("POST", url=parametros["url"], headers=headers, data=payload,timeout=120)
    return resposta


# cria dataframe que irá receber cada tabela tratada
relatorio_consolidado = pd.DataFrame(columns=TIPOS)

# consulta dicionários e listas para extrair relatório de forma dinâmica
for periodo_competencia in PERIODOS:
    for municipio in MUNICIPIOS:
        for categoria_profissional in PROFISSIONAIS:
            for atendimento in TIPO_ATENDIMENTO:
                resposta = realizar_requisicao(
                    ParametrosRequisicao(
                        estado_codigo = ESTADOS[municipio],
                        municipio_codigo = MUNICIPIOS[municipio],
                        periodo_competencia = periodo_competencia,
                        categoria_profissional_codigo = PROFISSIONAIS[categoria_profissional],
                        atendimento_tipo_codigo = TIPO_ATENDIMENTO[atendimento],
                        url = "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/saude/RelSauProducao.xhtml"
                    )
                )
                try:
                    relatorio_extraido = (pd.read_csv(StringIO(resposta.text),delimiter=';',header=11, encoding='ISO-8859-1'))
                    
                    # exclui colunas e valores ausentes
                    relatorio_extraido = relatorio_extraido.drop(["Unnamed: 13"], axis=1).dropna()
                  
                    # renomeia colunas 
                    relatorio_extraido = relatorio_extraido.rename(columns=COLUNAS_RENOMEAR)

                    # pivota tabela em função da coluna problema_condicao_avaliada
                    relatorio_extraido = pd.melt(relatorio_extraido.reset_index(), id_vars=['problema_condicao_avaliada'],var_name='conduta', value_name='quantidade')
                    relatorio_extraido = relatorio_extraido[relatorio_extraido.conduta.str.contains("index") == False]

                    # enriquece tabela com demais campos identificadores
                    relatorio_extraido['municipio_uf'] = ESTADOS[municipio]
                    relatorio_extraido['municipio_id_sus'] = MUNICIPIOS[municipio]
                    relatorio_extraido['municipio_nome'] = municipio
                    relatorio_extraido['periodo_data'] = periodo_competencia[4:7] + '/' + periodo_competencia[0:4] 
                    relatorio_extraido['categoria_profissional'] = categoria_profissional
                    relatorio_extraido['tipo_atendimento'] = TIPO_ATENDIMENTO[atendimento]
                    
                    # garante o tipo dos dados
                    relatorio_extraido = relatorio_extraido.astype(TIPOS)

                    # armazena tabela tratada na tabela consolidada receptora
                    relatorio_consolidado = relatorio_consolidado.append(relatorio_extraido)
                except:
                    pass

print(relatorio_consolidado)

#salva o relatório
relatorio_consolidado.to_csv(r"C:\Users\maira\Impulso\painel_de_produtividade\bloco_05_v2\tres_marias.csv", index=False, encoding='utf-8') 