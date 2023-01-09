import warnings
warnings.filterwarnings("ignore")
import requests
import pandas as pd
import numpy as np
import json

from impulsoetl.cnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.loggers import logger


def extrair_informacoes_estabelecimentos(codigo_municipio: str, lista_cnes: list) -> pd.DataFrame:
    
    colunas = [ 'noEmpresarial', 'natJuridica','natJuridicaMant', 'cnpj', 'tpPessoa', 'nvDependencia', 'nuAlvara',
       'dtExpAlvara', 'orgExpAlvara', 'dsTpUnidade', 'dsStpUnidade','noLogradouro', 'nuEndereco', 'cep', 'regionalSaude', 'bairro',
       'noComplemento', 'municipio', 'noMunicipio', 'uf', 'tpGestao','nuTelefone', 'tpSempreAberto', 'coMotivoDesab', 'dsMotivoDesab',
       'cpfDiretorCln', 'stContratoFormalizado', 'nuCompDesab', 'dtCarga','dtAtualizacaoOrigem', 'dtAtualizacao']
    
    df_extraido = pd.DataFrame()
    estabelecimentos_cnes_sem_ficha = []

    logger.info("Iniciando a extração das informações dos estabelecimentos do município: " + codigo_municipio)
    
    for cnes in lista_cnes:
        try:
            url = "http://cnes.datasus.gov.br/services/estabelecimentos/"+codigo_municipio+cnes
            payload={}
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Referer': 'http://cnes.datasus.gov.br/pages/estabelecimentos/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
            }

            response = requests.request("GET", url, headers=headers, data=payload)
            res = response.text
    
            parsed = json.loads(res)
            df_parcial = pd.DataFrame([parsed])

            df_extraido = df_extraido.append(df_parcial)

            logger.info("Extração concluída para o estabelecimento: " + cnes)
        
        except json.JSONDecodeError:
            logger.info("Não foi possível extrair as informações para o estabelecimento: " + cnes)
            estabelecimentos_cnes_sem_ficha.append(cnes)
            pass

    df_sem_ficha = pd.DataFrame(estabelecimentos_cnes_sem_ficha, columns=['cnes'])
    df_sem_ficha['id'] = np.nan
    df_sem_ficha['noFantasia'] = "NAO IDENTIFICADO"
    df_sem_ficha = df_sem_ficha[['id','cnes', 'noFantasia']]
    for coluna in colunas:
        df_sem_ficha[coluna] = np.nan
    
    df_extraido = df_extraido.append(df_sem_ficha)

    return df_extraido

#codigo_municipio = '110025'
#lista_cnes = extrair_lista_cnes(codigo_municipio)
#data = extrair_informacoes_estabelecimentos(codigo_municipio, lista_cnes)
#data.loc[data['id']==np.nan]
#print(data)