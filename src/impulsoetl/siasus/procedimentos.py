# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém dados de procedimentos ambulatoriais registrados no SIASUS."""


from __future__ import annotations

import os
from datetime import date
from typing import Final, Generator

import janitor  # noqa: F401  # nopycln: import
import numpy as np
import pandas as pd
from frozendict import frozendict
from sqlalchemy.orm import Session
from uuid6 import uuid7

from impulsoetl.comum.datas import agora_gmt_menos3, periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import logger
from impulsoetl.utilitarios.bd import carregar_dataframe
from impulsoetl.utilitarios.datasus_ftp import extrair_dbc_lotes

DE_PARA_PA: Final[frozendict] = frozendict(
    {
        "PA_CODUNI": "estabelecimento_id_scnes",
        "PA_GESTAO": "gestao_unidade_geografica_id_sus",
        "PA_CONDIC": "gestao_condicao_id_siasus",
        "PA_UFMUN": "unidade_geografica_id_sus",
        "PA_REGCT": "regra_contratual_id_scnes",
        "PA_INCOUT": "incremento_outros_id_sigtap",
        "PA_INCURG": "incremento_urgencia_id_sigtap",
        "PA_TPUPS": "estabelecimento_tipo_id_sigtap",
        "PA_TIPPRE": "prestador_tipo_id_sigtap",
        "PA_MN_IND": "estabelecimento_mantido",
        "PA_CNPJCPF": "estabelecimento_id_cnpj",
        "PA_CNPJMNT": "mantenedora_id_cnpj",
        "PA_CNPJ_CC": "receptor_credito_id_cnpj",
        "PA_MVM": "processamento_periodo_data_inicio",
        "PA_CMP": "realizacao_periodo_data_inicio",
        "PA_PROC_ID": "procedimento_id_sigtap",
        "PA_TPFIN": "financiamento_tipo_id_sigtap",
        "PA_SUBFIN": "financiamento_subtipo_id_sigtap",
        "PA_NIVCPL": "complexidade_id_siasus",
        "PA_DOCORIG": "instrumento_registro_id_siasus",
        "PA_AUTORIZ": "autorizacao_id_siasus",
        "PA_CNSMED": "profissional_id_cns",
        "PA_CBOCOD": "profissional_vinculo_ocupacao_id_cbo2002",
        "PA_MOTSAI": "desfecho_motivo_id_siasus",
        "PA_OBITO": "obito",
        "PA_ENCERR": "encerramento",
        "PA_PERMAN": "permanencia",
        "PA_ALTA": "alta",
        "PA_TRANSF": "transferencia",
        "PA_CIDPRI": "condicao_principal_id_cid10",
        "PA_CIDSEC": "condicao_secundaria_id_cid10",
        "PA_CIDCAS": "condicao_associada_id_cid10",
        "PA_CATEND": "carater_atendimento_id_siasus",
        "PA_IDADE": "usuario_idade",
        "IDADEMIN": "procedimento_idade_minima",
        "IDADEMAX": "procedimento_idade_maxima",
        "PA_FLIDADE": "compatibilidade_idade_id_siasus",
        "PA_SEXO": "usuario_sexo_id_sigtap",
        "PA_RACACOR": "usuario_raca_cor_id_siasus",
        "PA_MUNPCN": "usuario_residencia_municipio_id_sus",
        "PA_QTDPRO": "quantidade_apresentada",
        "PA_QTDAPR": "quantidade_aprovada",
        "PA_VALPRO": "valor_apresentado",
        "PA_VALAPR": "valor_aprovado",
        "PA_UFDIF": "atendimento_residencia_ufs_distintas",
        "PA_MNDIF": "atendimento_residencia_municipios_distintos",
        "PA_DIF_VAL": "procedimento_valor_diferenca_sigtap",
        "NU_VPA_TOT": "procedimento_valor_vpa",
        "NU_PA_TOT": "procedimento_valor_sigtap",
        "PA_INDICA": "aprovacao_status_id_siasus",
        "PA_CODOCO": "ocorrencia_id_siasus",
        "PA_FLQT": "erro_quantidade_apresentada_id_siasus",
        "PA_FLER": "erro_apac",
        "PA_ETNIA": "usuario_etnia_id_sus",
        "PA_VL_CF": "complemento_valor_federal",
        "PA_VL_CL": "complemento_valor_local",
        "PA_VL_INC": "incremento_valor",
        "PA_SRV_C": "servico_especializado_id_scnes",
        "PA_INE": "equipe_id_ine",
        "PA_NAT_JUR": "estabelecimento_natureza_juridica_id_scnes",
    },
)

TIPOS_PA: Final[frozendict] = frozendict(
    {
        "estabelecimento_id_scnes": "object",
        "gestao_unidade_geografica_id_sus": "object",
        "gestao_condicao_id_siasus": "object",
        "unidade_geografica_id_sus": "object",
        "regra_contratual_id_scnes": "object",
        "incremento_outros_id_sigtap": "object",
        "incremento_urgencia_id_sigtap": "object",
        "estabelecimento_tipo_id_sigtap": "object",
        "prestador_tipo_id_sigtap": "object",
        "estabelecimento_mantido": "bool",
        "estabelecimento_id_cnpj": "object",
        "mantenedora_id_cnpj": "object",
        "receptor_credito_id_cnpj": "object",
        "processamento_periodo_data_inicio": "datetime64[ns]",
        "realizacao_periodo_data_inicio": "datetime64[ns]",
        "procedimento_id_sigtap": "object",
        "financiamento_tipo_id_sigtap": "object",
        "financiamento_subtipo_id_sigtap": "object",
        "complexidade_id_siasus": "object",
        "instrumento_registro_id_siasus": "object",
        "autorizacao_id_siasus": "object",
        "profissional_id_cns": "object",
        "profissional_vinculo_ocupacao_id_cbo2002": "object",
        "desfecho_motivo_id_siasus": "object",
        "obito": "bool",
        "encerramento": "bool",
        "permanencia": "bool",
        "alta": "bool",
        "transferencia": "bool",
        "condicao_principal_id_cid10": "object",
        "condicao_secundaria_id_cid10": "object",
        "condicao_associada_id_cid10": "object",
        "carater_atendimento_id_siasus": "object",
        "usuario_idade": "Int64",
        "procedimento_idade_minima": "Int64",
        "procedimento_idade_maxima": "Int64",
        "compatibilidade_idade_id_siasus": "object",
        "usuario_sexo_id_sigtap": "object",
        "usuario_raca_cor_id_siasus": "object",
        "usuario_residencia_municipio_id_sus": "object",
        "quantidade_apresentada": "Int64",
        "quantidade_aprovada": "Int64",
        "valor_apresentado": "Float64",
        "valor_aprovado": "Float64",
        "atendimento_residencia_ufs_distintas": "bool",
        "atendimento_residencia_municipios_distintos": "bool",
        "procedimento_valor_diferenca_sigtap": "Float64",
        "procedimento_valor_vpa": "Float64",
        "procedimento_valor_sigtap": "Float64",
        "aprovacao_status_id_siasus": "object",
        "ocorrencia_id_siasus": "object",
        "erro_quantidade_apresentada_id_siasus": "object",
        "erro_apac": "object",
        "usuario_etnia_id_sus": "object",
        "complemento_valor_federal": "Float64",
        "complemento_valor_local": "Float64",
        "incremento_valor": "Float64",
        "servico_id_sigtap": "object",
        "servico_classificacao_id_sigtap": "object",
        "equipe_id_ine": "object",
        "estabelecimento_natureza_juridica_id_scnes": "object",
        "id": "object",
        "periodo_id": "object",
        "unidade_geografica_id": "object",
        "criacao_data": "datetime64[ns]",
        "atualizacao_data": "datetime64[ns]",
    },
)

COLUNAS_DATA_AAAAMM: Final[list[str]] = [
    "realizacao_periodo_data_inicio",
    "processamento_periodo_data_inicio",
]

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_PA.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]


def _para_booleano(valor: str) -> bool | float:
    """Transforma um valor binário '0' ou '1' em booleano. Suporta NaNs."""
    if valor == "0":
        return False
    elif valor == "1":
        return True
    else:
        return np.nan


def extrair_pa(
    uf_sigla: str,
    periodo_data_inicio: date,
    passo: int = 10000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai registros de procedimentos ambulatoriais do FTP do DataSUS.

    Argumentos:
        uf_sigla: Sigla da Unidade Federativa cujos procedimentos se pretende
            obter.
        periodo_data_inicio: Dia de início da competência desejada,
            representado como um objeto [`datetime.date`][].
        passo: Número de registros que devem ser convertidos em DataFrame a
            cada iteração.

    Gera:
        A cada iteração, devolve um objeto [`pandas.DataFrames`][] com um
        trecho do arquivo de procedimentos ambulatoriais lido e convertido.

    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`datetime.date`]: https://docs.python.org/3/library/datetime.html#date-objects
    """

    return extrair_dbc_lotes(
        ftp="ftp.datasus.gov.br",
        caminho_diretorio="/dissemin/publicos/SIASUS/200801_/Dados",
        arquivo_nome="PA{uf_sigla}{periodo_data_inicio:%y%m}.dbc".format(
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        ),
        passo=passo,
    )


def transformar_pa(
    sessao: Session,
    pa: pd.DataFrame,
    condicoes: str | None = None,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de procedimentos ambulatoriais do SIASUS.
    
    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        pa: objeto [`pandas.DataFrame`][] contendo os dados de um arquivo de
            disseminação de procedimentos ambulatoriais do SIASUS, conforme
            extraídos para uma unidade federativa e competência (mês) pela
            função [`extrair_pa()`][].
        condicoes: conjunto opcional de condições a serem aplicadas para
            filtrar os registros obtidos da fonte. O valor informado deve ser
            uma *string* com a sintaxe utilizada pelo método
            [`pandas.DataFrame.query()`][]. Por padrão, o valor do argumento é
            `None`, o que equivale a não aplicar filtro algum.

    Note:
        Para otimizar a performance, os filtros são aplicados antes de qualquer
        outra transformação nos dados, de forma que as condições fornecidas
        devem considerar que o nome, os tipos e os valores aparecem exatamente
        como registrados no arquivo de disseminação disponibilizado no FTP
        público do DataSUS. Verifique o [Informe Técnico][it-siasus] para mais
        informações.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`extrair_pa()`]: impulsoetl.siasus.procedimentos.extrair_pa
    [`pandas.DataFrame.query()`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
    [it-siasus]: https://drive.google.com/file/d/1DC5093njSQIhMHydYptlj2rMbrMF36y6
    """
    logger.info(
        "Transformando DataFrame com {num_registros_pa} procedimentos "
        + "ambulatoriais.",
        num_registros_pa=len(pa),
    )
    logger.debug(
        "Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB.",
        memoria_usada=pa.memory_usage(deep=True).sum() / 10 ** 6,
    )

    # aplica condições de filtragem dos registros
    if condicoes:
        pa = pa.query(condicoes, engine="python")
        logger.info(
            "Registros após aplicar confições de filtragem: {num_registros}.",
            num_registros=len(pa),
        )

    pa_transformada = (
        pa  # noqa: WPS221  # ignorar linha complexa no pipeline
        # renomear colunas
        .rename_columns(function=lambda col: col.strip())
        .rename_columns(DE_PARA_PA)
        # processar colunas com datas
        .transform_columns(
            COLUNAS_DATA_AAAAMM,
            function=lambda dt: pd.to_datetime(
                dt,
                format="%Y%m",
                errors="coerce",
            ),
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
        .transform_columns(
            [
                "regra_contratual_id_scnes",
                "incremento_outros_id_sigtap",
                "incremento_urgencia_id_sigtap",
                "mantenedora_id_cnpj",
                "receptor_credito_id_cnpj",
                "financiamento_subtipo_id_sigtap",
                "condicao_principal_id_cid10",
                "autorizacao_id_siasus",
                "profissional_id_cns",
                "condicao_principal_id_cid10",
                "condicao_secundaria_id_cid10",
                "condicao_associada_id_cid10",
                "desfecho_motivo_id_siasus",
                "usuario_sexo_id_sigtap",
                "usuario_raca_cor_id_siasus",
            ],
            function=lambda elemento: (
                np.nan
                if pd.notna(elemento)
                and all(digito == "0" for digito in elemento)
                else elemento
            ),
        )
        .transform_columns(
            [
                "carater_atendimento_id_siasus",
                "usuario_residencia_municipio_id_sus",
                "atendimento_residencia_ufs_distintas",
                "atendimento_residencia_municipios_distintos",
            ],
            function=lambda elemento: (
                np.nan
                if pd.isna(elemento)
                or all(digito == "9" for digito in elemento)
                else elemento
            ),
        )
        .update_where(
            "usuario_idade == '999'",
            target_column_name="usuario_idade",
            target_val=np.nan,
        )
        # processar colunas lógicas
        .transform_column(
            "estabelecimento_mantido",
            function=lambda elemento: True if elemento == "M" else False,
        )
        .transform_columns(
            [
                "obito",
                "encerramento",
                "permanencia",
                "alta",
                "transferencia",
                "atendimento_residencia_ufs_distintas",
                "atendimento_residencia_municipios_distintos",
            ],
            function=_para_booleano,
        )
        .update_where(
            "@pd.isna(desfecho_motivo_id_siasus)",
            target_column_name=[
                "obito",
                "encerramento",
                "permanencia",
                "alta",
                "transferencia",
            ],
            target_val=np.nan,
        )
        # separar código do serviço e código da classificação do serviço
        .transform_column(
            "servico_especializado_id_scnes",
            function=lambda cod: cod[:3] if pd.notna(cod) else np.nan,
            dest_column_name="servico_id_sigtap",
        )
        .transform_column(
            "servico_especializado_id_scnes",
            function=lambda cod: cod[3:] if pd.notna(cod) else np.nan,
            dest_column_name="servico_classificacao_id_sigtap",
        )
        .remove_columns("servico_especializado_id_scnes")
        # adicionar id
        .add_column("id", str())
        .transform_column("id", function=lambda _: uuid7().hex)
        # adicionar id do periodo
        .transform_column(
            "realizacao_periodo_data_inicio",
            function=lambda dt: periodo_por_data(sessao=sessao, data=dt).id,
            dest_column_name="periodo_id",
        )
        # adicionar id da unidade geografica
        .transform_column(
            "unidade_geografica_id_sus",
            function=lambda id_sus: id_sus_para_id_impulso(
                sessao=sessao,
                id_sus=id_sus,
            ),
            dest_column_name="unidade_geografica_id",
        )
        # adicionar datas de inserção e atualização
        .add_column("criacao_data", agora_gmt_menos3())
        .add_column("atualizacao_data", agora_gmt_menos3())
        # garantir tipos
        .change_type(
            # HACK: ver https://github.com/pandas-dev/pandas/issues/25472
            COLUNAS_NUMERICAS,
            "float",
        )
        .astype(TIPOS_PA)
    )
    logger.debug(
        "Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB.",
        memoria_usada=(
            pa_transformada.memory_usage(deep=True).sum() / 10 ** 6
        ),
    )
    return pa_transformada


def validar_pa(pa_transformada: pd.DataFrame) -> pd.DataFrame:
    assert isinstance(pa_transformada, pd.DataFrame), "Não é um DataFrame"
    assert len(pa_transformada) > 0, "DataFrame vazio."
    nulos_por_coluna = pa_transformada.applymap(pd.isna).sum()
    assert nulos_por_coluna["quantidade_apresentada"] == 0, (
        "A quantidade apresentada é um valor nulo."
    )
    assert nulos_por_coluna["quantidade_aprovada"] == 0, (
        "A quantidade aprovada é um valor nulo."
    )
    assert nulos_por_coluna["realizacao_periodo_data_inicio"] == 0, (
        "A competência de realização é um valor nulo."
    )


def obter_pa(
    sessao: Session,
    uf_sigla: str,
    periodo_data_inicio: date,
    tabela_destino: str,
    teste: bool = False,
    **kwargs,
) -> None:
    """Baixa, transforma e carrega dados de procedimentos ambulatoriais.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        uf_sigla: Sigla da Unidade Federativa cujos procedimentos se pretende
            obter.
        periodo_data_inicio: Dia de início da competência desejada,
            representado como um objeto [`datetime.date`][].
        tabela_destino: nome da tabela de destino, qualificado com o nome do
            schema (formato `nome_do_schema.nome_da_tabela`).
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.
        \\*\\*kwargs: Parâmetros adicionais definidos no agendamento da
            captura. Atualmente, apenas o parâmetro `condicoes` (do tipo `str`)
            é aceito, e repassado como argumento na função
            [`transformar_pa()`][].

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`sqlalchemy.engine.Row`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Row
    [`datetime.date`]: https://docs.python.org/3/library/datetime.html#date-objects
    [`transformar_pa()`]: impulsoetl.siasus.procedimentos.transformar_pa
    """
    logger.info(
        "Iniciando captura de procedimentos ambulatoriais para Unidade "
        + "Federativa '{}' na competencia de {:%m/%Y}.",
        uf_sigla,
        periodo_data_inicio,
    )

    # obter tamanho do lote de processamento
    passo = int(os.getenv("IMPULSOETL_LOTE_TAMANHO", 100000))

    pa_lotes = extrair_pa(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
        passo=passo,
    )

    contador = 0
    for pa_lote in pa_lotes:
        pa_transformada = transformar_pa(
            sessao=sessao, 
            pa=pa_lote,
            condicoes=kwargs.get("condicoes"),
        )
        try:
            validar_pa(pa_transformada)
        except AssertionError as mensagem:
            sessao.rollback()
            raise RuntimeError(
                "Dados inválidos encontrados após a transformação:"
                + " {}".format(mensagem),
            )

        carregamento_status = carregar_dataframe(
            sessao=sessao,
            df=pa_transformada,
            tabela_destino=tabela_destino,
            passo=None,
            teste=teste,
        )
        if carregamento_status != 0:
            sessao.rollback()
            raise RuntimeError(
                "Execução interrompida em razão de um erro no "
                + "carregamento."
            )
        contador += len(pa_lote)
        if teste and contador > 1000:
            logger.info("Execução interrompida para fins de teste.")
            break

    if teste:
        logger.info("Desfazendo alterações realizadas durante o teste...")
        sessao.rollback()
        logger.info("Todas transações foram desfeitas com sucesso!")
