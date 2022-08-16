# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


from __future__ import annotations

import json
from datetime import date, datetime
from io import StringIO

import pandas as pd
import requests
from requests.models import Response
from sqlalchemy import delete
from sqlalchemy.orm import Session
from uuid6 import uuid7

from impulsoetl.bd import tabelas
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import logger
from impulsoetl.sisab.parametros_requisicao import head


def obter_lista_periodos_inseridos(
    sessao: Session,
    tabela_alvo: str,
) -> list[datetime]:
    """Obtém lista de períodos da períodos que já constam na tabela

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        tabela_alvo: Tabela alvo da busca.

    Retorna:
        Lista de períodos que já constam na tabela destino.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """

    tabela = tabelas[tabela_alvo]
    periodos = sessao.query(tabela.c.periodo_codigo).distinct().all()
    sessao.commit()

    periodos_codigos = [periodo.periodo_codigo for periodo in periodos]
    logger.info("Leitura dos períodos inseridos no banco Impulso OK!")
    return periodos_codigos


def competencia_para_periodo_codigo(periodo_competencia: date) -> str:
    """Converte a data de início de uma no código do periodo padrão da Impulso.

    Argumentos:
        periodo_competencia: período de competência de determinado relatório

    Retorna:
        Código do período correspondente à data de início informada.

    Exemplos:
        ```py
        >>> competencia_para_periodo_codigo('202203')
        2022.M3
        ```
    """

    return "{:%Y}.M{}".format(periodo_competencia, periodo_competencia.month)


def obter_data_criacao(
    sessao: Session,
    tabela_alvo: str,
    periodo_codigo: str,
) -> datetime:
    """Obtém a data de criação do registro a partir do código do período.

    Argumentos:
        tabela_alvo: Tabela alvo da busca.
        periodo_codigo: Período de referência da data.

    Retorna:
        Data de criação do registro, como um objeto `datetime`.
    """

    tabela = tabelas[tabela_alvo]

    data_criacao_obj = (
        sessao.query(tabela)
        .filter(tabela.c.periodo_codigo == periodo_codigo)
        .first()
    )
    sessao.commit()

    try:
        return data_criacao_obj.criacao_data  # type: ignore
    except AttributeError:
        return datetime.now()


def requisicao_validacao_sisab_producao(
    periodo_competencia: date,
    envio_prazo: bool,
) -> Response:
    """Obtém os dados da API do SISAB.

    Argumentos:
        periodo_competencia: Período de competência do dado a ser buscado no
            SISAB.
        envio_prazo: Indica se os relatórios de validação a serem considerados
            apenas os enviados no prazo (`True`) ou se devem considerar tanto
            envios no prazo quanto fora do prazo (`False`).

    Retorna:
        Resposta da requisição do SISAB, com os dados obtidos ou não.
    """

    url = (
        "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal"
        + "/envio/RelValidacao.xhtml"
    )
    periodo_tipo = "producao"
    hd = head(url)
    vs = hd[1]  # viewstate

    # Check box envio requisições no prazo marcado?
    envio_prazo_on = ""
    if envio_prazo:
        envio_prazo_on += "&envioPrazo=on"

    payload = (
        "j_idt44=j_idt44&unidGeo=brasil&periodo="
        + periodo_tipo
        + "&j_idt70="
        + "{:%Y%m}".format(periodo_competencia)
        + "&colunas=regiao&colunas=uf&colunas=ibge&colunas=municipio"
        + "&colunas=cnes&colunas=ine"
        + envio_prazo_on
        + "&javax.faces.ViewState="
        + vs
        + "&j_idt102=j_idt102"
    )
    headers = hd[0]
    resposta = requests.request("POST", url, headers=headers, data=payload)
    logger.info("Dados Obtidos no SISAB")
    return resposta


def tratamento_validacao_producao(
    sessao: Session,
    resposta: Response,
    data_criacao: datetime,
    envio_prazo: bool,
    periodo_codigo: str,
) -> pd.DataFrame:
    """Tratamento dos dados obtidos

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        resposta: Resposta da requisição efetuada no SISAB.
        data_criacao: data de criação do registro inserido.
        envio_prazo: Indica se os relatórios de validação a serem considerados
            apenas os enviados no prazo (`True`) ou se devem considerar tanto
            envios no prazo quanto fora do prazo (`False`).
        periodo_codigo: Código do período de referência.

    Retorna:
        Objeto [`pandas.DataFrame`] com os dados enriquecidos e tratados.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    """

    logger.info("Dados em tratamento")

    envio_prazo_on = "&envioPrazo=on"

    df = pd.read_csv(
        StringIO(resposta.text),
        sep=";",
        encoding="ISO-8859-1",
        skiprows=range(0, 4),
        skipfooter=4,
        engine="python",
    )  # ORIGINAL DIRETO DA EXTRAÇÃO

    df["INE"] = df["INE"].fillna("0")

    df["INE"] = df["INE"].astype("int")

    assert df["Uf"].count() > 26, "Estado faltante"

    df.drop(["Região", "Uf", "Municipio", "Unnamed: 8"], axis=1, inplace=True)

    df.columns = [
        "municipio_id_sus",
        "cnes_id",
        "ine_id",
        "validacao_nome",
        "validacao_quantidade",
    ]

    # novas colunas em lugares específicos
    df.insert(0, "id", value="")

    df.insert(2, "periodo_id", value="")

    # novas colunas para padrão tabela requerida
    df = df.assign(
        criacao_data=data_criacao,
        atualizacao_data=pd.Timestamp.now(),
        no_prazo=1 if (envio_prazo == envio_prazo_on) else 0,
        periodo_codigo=periodo_codigo,
    )

    tabela_periodos = tabelas["listas_de_codigos.periodos"]
    periodo_id = (
        sessao.query(tabela_periodos)  # type: ignore
        .filter(tabela_periodos.c.codigo == periodo_codigo)
        .first()
        .id
    )
    sessao.commit()

    df = df.assign(periodo_id=periodo_id)

    df["id"] = df.apply(lambda row: uuid7(), axis=1)

    df.insert(11, "unidade_geografica_id", value="")

    df["unidade_geografica_id"] = df["municipio_id_sus"].apply(
        lambda row: id_sus_para_id_impulso(sessao, id_sus=row)
    )

    df[
        [
            "id",
            "municipio_id_sus",
            "periodo_id",
            "cnes_id",
            "ine_id",
            "validacao_nome",
            "periodo_codigo",
            "unidade_geografica_id",
        ]
    ] = df[
        [
            "id",
            "municipio_id_sus",
            "periodo_id",
            "cnes_id",
            "ine_id",
            "validacao_nome",
            "periodo_codigo",
            "unidade_geografica_id",
        ]
    ].astype(
        "string"
    )

    df["validacao_quantidade"] = df["validacao_quantidade"].astype("int")

    df["no_prazo"] = df["no_prazo"].astype("bool")

    df["atualizacao_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_validacao_tratado = df
    logger.info("Dados tratados")
    logger.debug(df_validacao_tratado.head())
    return df_validacao_tratado


def carregar_validacao_producao(
    sessao: Session,
    df_validacao_tratado: pd.DataFrame,
    periodo_competencia: date,
    tabela_destino: str,
) -> int:
    """Carrega os dados de um arquivo validação do SISAB no banco da Impulso.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        relatorio_validacao_df: objeto [`pandas.DataFrame`][] contendo os
            dados a serem carregados na tabela de destino, já no formato
            utilizado pelo banco de dados da ImpulsoGov.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    """

    relatorio_validacao_df = df_validacao_tratado

    registros = json.loads(
        relatorio_validacao_df.to_json(
            orient="records",
            date_format="iso",
        )
    )

    tabela_relatorio_validacao = tabelas[tabela_destino]

    periodo_codigo = competencia_para_periodo_codigo(periodo_competencia)

    periodos_inseridos = obter_lista_periodos_inseridos(sessao, tabela_destino)

    if periodo_codigo in periodos_inseridos:

        limpar = delete(tabela_relatorio_validacao).where(
            tabela_relatorio_validacao.c.periodo_codigo == periodo_codigo
        )
        logger.debug(limpar)
        sessao.execute(limpar)

    requisicao_insercao = tabela_relatorio_validacao.insert().values(registros)

    sessao.execute(requisicao_insercao)

    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome=tabela_destino,
        linhas_adicionadas=len(relatorio_validacao_df),
    )

    return 0


def testes_pre_carga(df_validacao_tratado: pd.DataFrame) -> None:
    """Realiza algumas validações no dataframe antes da carga ao banco.

    Argumentos:
        relatorio_validacao_df: objeto [`pandas.DataFrame`][] contendo os
            dados a serem carregados na tabela de destino, já no formato
            utilizado pelo banco de dados da ImpulsoGov.

    Exceções:
        Levanta uma exceção da classe [`AssertionError`][] se algum teste de
        qualidade dos dados não for atendido.

    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`AssertionError`]: https://docs.python.org/3/library/exceptions.html#AssertionError
    """

    assert (
        df_validacao_tratado["municipio_id_sus"].nunique() > 5000
    ), "Número de municípios obtidos menor que 5000"

    assert (
        df_validacao_tratado["unidade_geografica_id"].nunique()
        == df_validacao_tratado["municipio_id_sus"].nunique()
    ), "Falta de unidade geográfica"

    assert (
        sum(df_validacao_tratado["cnes_id"].isna()) == 0
    ), "Dado ausente em cnes_id"

    assert (
        sum(df_validacao_tratado["id"].isna()) == 0
    ), "Id do registro ausente"

    assert (
        sum(df_validacao_tratado["validacao_nome"].isna()) == 0
    ), "Nome da validacão ausente"

    assert (
        sum(df_validacao_tratado["validacao_quantidade"]) > 0
    ), "Quantidade de validação inválida"

    assert (
        len(df_validacao_tratado.columns) == 12
    ), "Falta de coluna no dataframe"

    logger.info("Testes OK!")
    return None


def obter_validacao_municipios_producao(
    sessao: Session,
    periodo_competencia: date,
    envio_prazo: bool,
    tabela_destino: str,
    periodo_codigo: str,
) -> None:
    """Executa o ETL de relatórios de validação dos envios ao SISAB.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        periodo_competencia: Data de início do período de referência do dado.
        envio_prazo: Indica se os relatórios de validação a serem considerados
            apenas os enviados no prazo (`True`) ou se devem considerar tanto
            envios no prazo quanto fora do prazo (`False`).
        tabela_destino: Nome da tabela no banco de dados da ImpulsoGov para
            onde serão carregados os dados capturados (no formato
            `nome_do_schema.nome_da_tabela`).
        periodo_codigo: Código do período de referência.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """

    data_criacao = obter_data_criacao(sessao, tabela_destino, periodo_codigo)

    resposta = requisicao_validacao_sisab_producao(
        periodo_competencia,
        envio_prazo,
    )

    df_validacao_tratado = tratamento_validacao_producao(
        sessao,
        resposta,
        data_criacao,
        envio_prazo,
        periodo_codigo,
    )

    testes_pre_carga(df_validacao_tratado)

    carregar_validacao_producao(
        sessao,
        df_validacao_tratado,
        periodo_competencia,
        tabela_destino,
    )

    logger.info("Dados prontos para o commit")
    return None
