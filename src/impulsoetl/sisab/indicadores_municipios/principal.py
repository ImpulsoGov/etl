from __future__ import annotations
from typing import Final
from sqlalchemy.orm import Session
from datetime import date
from impulsoetl.sisab.indicadores_municipios.extracao import (extrair_indicadores)
from impulsoetl.sisab.indicadores_municipios.tratamento import (tratamento_dados)
from impulsoetl.sisab.indicadores_municipios.teste_validacao import (teste_validacao)
from impulsoetl.sisab.indicadores_municipios.carregamento import (carregar_indicadores)

INDICADORES_CODIGOS : Final[dict[str, str]] = {
    "Pré-Natal (6 consultas)":"1",
    "Pré-Natal (Sífilis e HIV)":"2",
    "Gestantes Saúde Bucal":"3",
    "Cobertura Citopatológico":"4",
    "Cobertura Polio e Penta":"5",
    "Hipertensão (PA Aferida)":"6",
    "Diabetes (Hemoglobina Glicada)":"7"
    }

def obter_indicadores_desempenho(
    sessao: Session,
    visao_equipe: str,
    quadrimestre: date,
    teste:bool = False
) -> None:
        for indicador in INDICADORES_CODIGOS:
            df = extrair_indicadores(visao_equipe=visao_equipe,quadrimestre=quadrimestre,indicador=indicador)
            df_tratado = tratamento_dados(sessao=sessao,dados_sisab_indicadores=df,periodo=quadrimestre,indicador=indicador)
            teste_validacao(df,df_tratado,indicador)
            carregar_indicadores(sessao=sessao,indicadores_transformada=df_tratado,visao_equipe=visao_equipe)

