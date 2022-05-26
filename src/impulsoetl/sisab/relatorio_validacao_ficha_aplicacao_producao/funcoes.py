# flake8: noqa
# type: ignore
import json
from datetime import date, datetime
from io import StringIO

import pandas as pd
import requests
from requests import Response
from sqlalchemy import delete
from sqlalchemy.orm import Query, Session

from impulsoetl.bd import tabelas
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import logger
from impulsoetl.sisab.relatorio_validacao_ficha_aplicacao_producao.suporte_extracao import (
    head,
)


def obter_lista_registros_inseridos(
    sessao: Session,
    tabela_destino: str,
) -> Query:
    """Obtém lista de registro da períodos que já constam na tabela.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
        acessar a base de dados da ImpulsoGov.
        tabela_destino: Tabela que irá acondicionar os dados.


    Retorna:
        Lista de períodos que já constam na tabela destino filtrados por ficha
        e aplicação.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """

    tabela = tabelas[tabela_destino]
    registros = sessao.query(
        tabela.c.periodo_codigo, tabela.c.ficha, tabela.c.aplicacao
    ).distinct(tabela.c.periodo_codigo, tabela.c.ficha, tabela.c.aplicacao)

    logger.info("Leitura dos períodos inseridos no banco Impulso OK!")
    return registros


def obter_data_criacao(
    sessao: Session,
    tabela_destino: str,
    periodo_codigo: str,
) -> datetime:
    """Obtém a data de criação do registro a partir do código do período.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite acessar
            a base de dados da Impulso Gov.
        tabela_destino: Tabela destino dos dados alvo da busca.
        periodo_codigo: Código do período de referência.

    Retorna:
        Data de criação do registro, como um objeto [`datetime`][].

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`datetime`]: https://docs.python.org/3/library/datetime.html#datetime-objects
    """

    tabela = tabelas[tabela_destino]

    data_criacao_obj = (
        sessao.query(tabela)
        .filter(tabela.c.periodo_codigo == periodo_codigo)
        .first()
    )

    try:
        return data_criacao_obj.criacao_data  # type: ignore
    except AttributeError:
        return datetime.now()


def requisicao_validacao_sisab_producao_ficha_aplicacao(
    periodo_competencia: date,
    ficha_codigo: str,
    aplicacao_codigo: str,
    envio_prazo: bool,
) -> Response:

    """Obtém relatórios de validação do SISAB, por ficha e por aplicação. 

    Argumentos:
        periodo_competencia: Período de competência do dado a ser buscado no
            SISAB.
        ficha_codigo: Código da ficha a ser preenchida na requisição
        aplicacao_codigo: Código da aplicação a ser preenchida na requisição.
        envio_prazo: Tipo de relatório de validação a ser obtido (referência
            check box "no prazo" no SISAB).

    Retorna:
        Resposta da requisição do SISAB, com os dados obtidos ou não.
    """
    periodo_competencia_AAAAMM = "{:%Y%m}".format(periodo_competencia)
    print(periodo_competencia_AAAAMM)

    if envio_prazo == True:
        envio_tipo = "&envioPrazo=on"
    else:
        envio_tipo = ""

    logger.info("iniciando conexão com o SISAB")
    url = "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/envio/RelValidacao.xhtml"
    periodo_tipo = "producao"
    hd = head(url)
    vs = hd[1]  # viewstate
    payload = (
        "j_idt44=j_idt44&unidGeo=brasil&periodo="
        + periodo_tipo
        + "&j_idt70="
        + periodo_competencia_AAAAMM
        + "&colunas=regiao&colunas=uf&colunas=ibge&colunas=municipio"
        + "&colunas=cnes&colunas=tp_unidade&colunas=ine&colunas=tp_equipe"
        + ficha_codigo
        + aplicacao_codigo
        + envio_tipo
        + "&javax.faces.ViewState="
        + vs
        + "&j_idt102=j_idt102"
    )
    headers = hd[0]
    resposta = requests.request("POST", url, headers=headers, data=payload)
    logger.info("Dados Obtidos no SISAB")
    return resposta


def tratamento_validacao_producao_ficha_aplicacao(
    sessao: Session,
    data_criacao: datetime,
    resposta: Response,
    ficha_tipo: str,
    aplicacao_tipo: str,
    envio_prazo: str,
    periodo_codigo: str,
) -> pd.DataFrame:
    """Tratamento dos dados obtidos

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite acessar
            a base de dados da Impulso Gov.
        data_criacao: Data de criação da tabela alvo
        resposta: Resposta da requisição efetuada no sisab
        ficha_tipo: Nome da ficha requisitada
        aplicacao_tipo: Nome da aplicacao requisitada
        envio_prazo: Tipo de relatório de validação a ser obtido (referência
            check box "no prazo" no SISAB.)


    Retorna:
        Objeto [`pandas.DataFrame`][] com os dados enriquecidos e tratados.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    """

    tabela_periodos = tabelas["listas_de_codigos.periodos"]

    logger.info("Dados em tratamento")

    df_obtido = pd.read_csv(
        StringIO(resposta.text),
        sep=";",
        encoding="ISO-8859-1",
        skiprows=range(0, 6),
        skipfooter=4,
        engine="python",
    )  # ORIGINAL DIRETO DA EXTRAÇÃO

    assert df_obtido["Uf"].count() > 26, "Estado faltante"

    df_obtido[["INE", "Tipo Unidade", "Tipo Equipe"]] = (
        df_obtido[["INE", "Tipo Unidade", "Tipo Equipe"]]
        .fillna("0")
        .astype("int")
    )

    colunas = [
        "municipio_id_sus",
        "periodo_id",
        "cnes_id",
        "cnes_nome",
        "ine_id",
        "ine_tipo",
        "ficha",
        "aplicacao",
        "validacao_nome",
        "validacao_quantidade",
        "criacao_data",
        "atualizacao_data",
        "periodo_codigo",
        "no_prazo",
        "municipio_nome",
        "unidade_geografica_id",
    ]

    df = pd.DataFrame(columns=colunas)

    periodo_id = (
        sessao.query(tabela_periodos)  # type: ignore
        .filter(tabela_periodos.c.codigo == periodo_codigo)
        .first()
        .id
    )
    sessao.commit()

    df["municipio_id_sus"] = df_obtido["IBGE"]

    df["periodo_id"] = periodo_id

    df["cnes_id"] = df_obtido["CNES"]

    df["cnes_nome"] = df_obtido["Tipo Unidade"]

    df["ine_id"] = df_obtido["INE"]

    df["ine_tipo"] = df_obtido["Tipo Equipe"]

    df["validacao_nome"] = df_obtido["Validação"]

    df["validacao_quantidade"] = df_obtido["Total"]

    df["municipio_nome"] = df_obtido["Municipio"]

    df["atualizacao_data"] = pd.Timestamp.now()

    df["criacao_data"] = data_criacao

    df["periodo_codigo"] = periodo_codigo

    df["no_prazo"] = 1 if (envio_prazo == True) else 0

    df["ficha"] = ficha_tipo

    df["aplicacao"] = aplicacao_tipo

    df["unidade_geografica_id"] = df["municipio_id_sus"].apply(
        lambda row: id_sus_para_id_impulso(sessao, id_sus=row)
    )

    df[
        [
            "municipio_id_sus",
            "periodo_id",
            "cnes_id",
            "cnes_nome",
            "ine_id",
            "ine_tipo",
            "ficha",
            "aplicacao",
            "validacao_nome",
            "periodo_codigo",
            "municipio_nome",
        ]
    ] = df[
        [
            "municipio_id_sus",
            "periodo_id",
            "cnes_id",
            "cnes_nome",
            "ine_id",
            "ine_tipo",
            "ficha",
            "aplicacao",
            "validacao_nome",
            "periodo_codigo",
            "municipio_nome",
        ]
    ].astype(
        "string"
    )

    df["validacao_quantidade"] = df["validacao_quantidade"].astype("int")
    df["no_prazo"] = df["no_prazo"].astype("bool")

    df["atualizacao_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df["criacao_data"] = data_criacao

    df_validacao_tratado = df

    logger.info("Dados tratados")

    print(df_validacao_tratado.head())

    return df_validacao_tratado


def testes_pre_carga_validacao_ficha_aplicacao_producao(
    df_validacao_tratado: pd.DataFrame,
) -> None:
    """Realiza algumas validações no dataframe antes da carga ao banco.

    Argumentos:
            df_validacao_tratado: objeto [`pandas.DataFrame`][] contendo os
                dados a serem carregados na tabela de destino, já no formato
                utilizado pelo banco de dados da Impulso Gov.
    
    Exceções:
        Levanta uma exceção `AssertionError` caso alguma das validações falhe.

    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    """

    assert all(
        [
            sum(df_validacao_tratado["cnes_id"].isna()) == 0,
            "Dado ausente em cnes_id",
            sum(df_validacao_tratado["ine_id"].isna()) == 0,
            "ine_id ausente",
            sum(df_validacao_tratado["ficha"].isna()) == 0,
            "Nome da ficha ausente",
            sum(df_validacao_tratado["aplicacao"].isna()) == 0,
            "Nome da aplicacao ausente",
            sum(df_validacao_tratado["validacao_nome"].isna()) == 0,
            "Nome da validacão ausente",
            sum(df_validacao_tratado["municipio_nome"].isna()) == 0,
            "Nome de município ausente",
            df_validacao_tratado["unidade_geografica_id"].nunique()
            == df_validacao_tratado["municipio_id_sus"].nunique(),
            "Falta de unidade geográfica",
            sum(df_validacao_tratado["validacao_quantidade"]) > 0,
            "Quantidade de validação inválida",
            len(df_validacao_tratado.columns) == 16,
            "Falta de coluna no dataframe",
        ]
    )

    logger.info("Testes OK!")
    return None


def carregar_validacao_ficha_aplicacao_producao(
    sessao: Session,
    df_validacao_tratado: pd.DataFrame,
    periodo_codigo: str,
    ficha_tipo: str,
    aplicacao_tipo: str,
    tabela_destino: str,
) -> int:
    """Carrega relatório de validação por ficha e aplicação no BD da Impulso.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        df_validacao_tratado: objeto [`pandas.DataFrame`][] contendo os dados a 
            serem carregados na tabela de destino, já no formato utilizado pelo
            banco de dados da Impulso Gov.
        periodo_codigo: Código do período de referência.
        ficha_tipo: Nome da ficha requisitada
        aplicacao_tipo: Nome da aplicacao requisitada
        tabela_destino: Tabela que irá acondicionar os dados.

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

    registros_inseridos = obter_lista_registros_inseridos(
        sessao, tabela_destino
    )

    if any(
        [
            registro.aplicacao == aplicacao_tipo
            and registro.ficha == ficha_tipo
            and registro.periodo_codigo == periodo_codigo
            for registro in registros_inseridos
        ]
    ):
        limpar = (
            delete(tabela_relatorio_validacao)
            .where(
                tabela_relatorio_validacao.c.periodo_codigo == periodo_codigo
            )
            .where(tabela_relatorio_validacao.c.ficha == ficha_tipo)
            .where(tabela_relatorio_validacao.c.aplicacao == aplicacao_tipo)
        )
        logger.debug(limpar)
        sessao.execute(limpar)

    requisicao_insercao = tabela_relatorio_validacao.insert().values(registros)
    sessao.execute(requisicao_insercao)

    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome=tabelas[tabela_destino],
        linhas_adicionadas=len(relatorio_validacao_df),
    )

    return 0


def obter_validacao_ficha_aplicacao_producao(
    sessao: Session,
    periodo_competencia: date,
    ficha_tipo: str,
    aplicacao_tipo: str,
    ficha_codigo: str,
    aplicacao_codigo: str,
    envio_prazo: bool,
    tabela_destino: str,
    periodo_codigo: str,
) -> None:
    """Executa o ETL de relatórios de validação por ficha e aplicação do SISAB.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        ficha_tipo: Nome da ficha requisitada
        aplicacao_tipo: Nome da aplicacao requisitada
        ficha_codigo: Código da ficha a ser preenchida na requisição
        aplicacao_codigo: Código da aplicação a ser preenchida na requisição.
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

    resposta = requisicao_validacao_sisab_producao_ficha_aplicacao(
        periodo_competencia=periodo_competencia,
        ficha_codigo=ficha_codigo,
        aplicacao_codigo=aplicacao_codigo,
        envio_prazo=envio_prazo,
    )

    df_validacao_tratado = tratamento_validacao_producao_ficha_aplicacao(
        sessao=sessao,
        resposta=resposta,
        data_criacao=data_criacao,
        ficha_tipo=ficha_tipo,
        aplicacao_tipo=aplicacao_tipo,
        envio_prazo=envio_prazo,
        periodo_codigo=periodo_codigo,
    )

    testes_pre_carga_validacao_ficha_aplicacao_producao(
        df_validacao_tratado=df_validacao_tratado
    )

    carregar_validacao_ficha_aplicacao_producao(
        sessao=sessao,
        df_validacao_tratado=df_validacao_tratado,
        periodo_codigo=periodo_codigo,
        ficha_tipo=ficha_tipo,
        aplicacao_tipo=aplicacao_tipo,
        tabela_destino=tabela_destino,
    )

    logger.info("Dados prontos para o commit")
    return None
