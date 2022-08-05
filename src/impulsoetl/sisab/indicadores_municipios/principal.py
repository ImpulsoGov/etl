from __future__ import annotations
from typing import Final
from sqlalchemy.orm import Session
from datetime import date
from bd import Sessao
from impulsoetl.sisab.indicadores_municipios.log import logger
from impulsoetl.sisab.indicadores_municipios.extracao import (extrair_dados)
from impulsoetl.sisab.indicadores_municipios.tratamento import (tratamento_dados)
from impulsoetl.sisab.indicadores_municipios.teste_validacao import (teste_validacao)
from impulsoetl.sisab.indicadores_municipios.carregamento import (carregar_indicadores)

INDICADORES_CODIGOS : Final[dict[str, str]] = {
    "Pré-Natal (6 consultas)":"10",
    "Pré-Natal (Sífilis e HIV)":"20",
    "Gestantes Saúde Bucal":"30",
    "Cobertura Citopatológico":"40",
    "Cobertura Polio e Penta":"50"
    }

def obter_indicadores_desempenho(
    sessao: Session,
    visao_equipe: str,
    quadrimestre: date,
    teste:bool = False
) -> None:

        """ Extrai, transforma e carrega os dados do relatório de indicadores do SISAB.
            Argumentos:
                sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
                    acessar a base de dados da ImpulsoGov.
                visao_equipe: Status das equipes consideradas no cálculo dos indicadores.
                quadrimestre: Data do quadrimestre da competência em referência.
        """
        for indicador in INDICADORES_CODIGOS:
                df_extraido = extrair_dados(visao_equipe=visao_equipe,quadrimestre=quadrimestre,indicador=indicador)
                df_tratado = tratamento_dados(sessao=sessao,df_extraido=df_extraido,periodo=quadrimestre,indicador=indicador)
                teste_validacao(df_extraido,df_tratado,indicador)
                carregar_indicadores(sessao=sessao,indicadores_transformada=df_tratado,visao_equipe=visao_equipe)
      