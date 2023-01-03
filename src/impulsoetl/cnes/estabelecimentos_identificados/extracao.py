import requests
import pandas as pd
import json

import sys

sys.path.append (r'C:\Users\maira\Impulso\etl\src\impulsoetl')
from cnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.loggers import logger


def extrair_informacoes_estabelecimentos(codigo_municipio: str, lista_cnes: list) -> pd.DataFrame:
    
    colunas = ['id', 'cnes', 'noFantasia', 'noEmpresarial', 'natJuridica',
       'natJuridicaMant', 'cnpj', 'tpPessoa', 'nvDependencia', 'nuAlvara',
       'dtExpAlvara', 'orgExpAlvara', 'dsTpUnidade', 'dsStpUnidade',
       'noLogradouro', 'nuEndereco', 'cep', 'regionalSaude', 'bairro',
       'noComplemento', 'municipio', 'noMunicipio', 'uf', 'tpGestao',
       'nuTelefone', 'tpSempreAberto', 'coMotivoDesab', 'dsMotivoDesab',
       'cpfDiretorCln', 'stContratoFormalizado', 'nuCompDesab', 'dtCarga',
       'dtAtualizacaoOrigem', 'dtAtualizacao']
    df_estabelecimentos = pd.DataFrame(columns=colunas)
    
    for cnes in lista_cnes:
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
        parsed = {k:[v] for k,v in parsed.items()}
        df = pd.DataFrame(parsed)
        df_estabelecimentos = df_estabelecimentos.append(df)

    return df_estabelecimentos


#coMun = '120001'
#lista_codigos = extrair_lista_cnes(coMun)
#data = extrair_informacoes_estabelecimentos(coMun, lista_codigos)
#print(data)