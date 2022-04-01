# flake8: noqa
from sqlalchemy.orm import Session
from datetime import datetime
from impulsoetl.bd import Sessao, tabelas
from impulsoetl.loggers import logger
import json


def carregar_relatorio_validacao(sessao: Session, relatorio_validacao_df) -> int:
    """Carrega os dados de um arquivo validação do portal SISAB no BD da Impulso.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        relatorio_validacao_df: [`DataFrame`][] contendo os dados a serem carregados
            na tabela de destino, já no formato utilizado pelo banco de dados
            da ImpulsoGov.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.

#     """
    registros = json.loads(
        relatorio_validacao_df.to_json(
            orient="records",
            date_format="iso",
        )
    )


    tabela_relatorio_validacao = tabelas[
        "dados_publicos._sisab_validacao_municipios_por_producao"
    ]  # tabela teste

    requisicao_insercao = tabela_relatorio_validacao.insert().values(registros)

    try:
        conector = sessao.connection()
        conector.execute(requisicao_insercao)
        sessao.commit()

        
        logger.info(
            "Carregamento concluído para a tabela `{tabela_nome}`: "
            + "adicionadas {linhas_adicionadas} novas linhas.",
            tabela_nome="dados_publicos._sisab_validacao_municipios_por_producao", 
            linhas_adicionadas=len(relatorio_validacao_df))
        
        

    except Exception as e:
        sessao.rollback()
        logger.info(e)
    
        

