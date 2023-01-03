import pandas as pd
import numpy as np
from typing import Final
from frozendict import frozendict

import sys

sys.path.append (r'C:\Users\maira\Impulso\etl\src\impulsoetl')
from cnes.extracao_lista_cnes import extrair_lista_cnes
from cnes.estabelecimentos_identificados.extracao import extrair_informacoes_estabelecimentos
from cnes.estabelecimentos_identificados.tratamento import tratamento_dados

coMun = '120001'
lista_cnes = extrair_lista_cnes(coMun)
df_extraido = extrair_informacoes_estabelecimentos(coMun,lista_cnes)
df_tratado = tratamento_dados(df_extraido)
print(df_tratado)