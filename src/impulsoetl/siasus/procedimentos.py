# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém dados de procedimentos ambulatoriais registrados no SIASUS."""


from __future__ import annotations

import json
import uuid
from typing import Final

import janitor  # noqa: F401  # nopycln: import
import numpy as np
import pandas as pd
from frozendict import frozendict
from pysus.online_data.SIA import download
from sqlalchemy.orm import Session

from impulsoetl.comum.datas import periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import logger
from impulsoetl.siasus.modelos import procedimentos as tabela_destino

DE_PARA_PA: Final[frozendict] = frozendict(
    {
        "PA_CODUNI": "estabelecimento_id_cnes",
        "PA_GESTAO": "gestao_unidade_geografica_id",
        "PA_CONDIC": "gestao_condicao_id_siasus",
        "PA_UFMUN": "unidade_geografica_id_sus",
        "PA_REGCT": "regra_contratual_id_cnes",
        "PA_INCOUT": "incremento_outros_id_sigtap",
        "PA_INCURG": "incremento_urgencia_id_sigtap",
        "PA_TPUPS": "estabelecimento_tipo_id_sigtap",
        "PA_TIPPRE": "prestador_tipo_id_sigtap",
        "PA_MN_IND": "estabelecimento_mantido",
        "PA_CNPJCPF": "estabelecimento_cnpj",
        "PA_CNPJMNT": "mantenedora_cnpj",
        "PA_CNPJ_CC": "receptor_credito_cnpj",
        "PA_MVM": "processamento_periodo_data_inicio",
        "PA_CMP": "realizacao_periodo_data_inicio",
        "PA_PROC_ID": "procedimento_id_sigtap",
        "PA_TPFIN": "financiamento_tipo_id_sigtap",
        "PA_SUBFIN": "financiamento_subtipo_id_sigtap",
        "PA_NIVCPL": "complexidade_id_siasus",
        "PA_DOCORIG": "instrumento_registro_id_siasus",
        "PA_AUTORIZ": "autorizacao_id_siasus",
        "PA_CNSMED": "profissional_cns",
        "PA_CBOCOD": "profissional_ocupacao_id_cbo",
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
        "PA_SRV_C": "servico_especializado_id_cnes",
        "PA_INE": "equipe_id_ine",
        "PA_NAT_JUR": "estabelecimento_natureza_juridica_id_cnes",
    },
)

TIPOS_PA: Final[frozendict] = frozendict(
    {
        "estabelecimento_id_cnes": "object",
        "gestao_unidade_geografica_id": "object",
        "gestao_condicao_id_siasus": "object",
        "unidade_geografica_id_sus": "object",
        "regra_contratual_id_cnes": "object",
        "incremento_outros_id_sigtap": "object",
        "incremento_urgencia_id_sigtap": "object",
        "estabelecimento_tipo_id_sigtap": "object",
        "prestador_tipo_id_sigtap": "object",
        "estabelecimento_mantido": "bool",
        "estabelecimento_cnpj": "object",
        "mantenedora_cnpj": "object",
        "receptor_credito_cnpj": "object",
        "processamento_periodo_data_inicio": "datetime64[ns]",
        "realizacao_periodo_data_inicio": "datetime64[ns]",
        "procedimento_id_sigtap": "object",
        "financiamento_tipo_id_sigtap": "object",
        "financiamento_subtipo_id_sigtap": "object",
        "complexidade_id_siasus": "object",
        "instrumento_registro_id_siasus": "object",
        "autorizacao_id_siasus": "object",
        "profissional_cns": "object",
        "profissional_ocupacao_id_cbo": "object",
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
        "estabelecimento_natureza_juridica_id_cnes": "object",
        "id": "object",
        "periodo_id": "object",
        "unidade_geografica_id": "object",
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


def transformar_pa(
    sessao: Session,
    pa: pd.DataFrame,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de procedimentos ambulatoriais do SIASUS."""
    logger.info(
        "Transformando DataFrame com {num_registros_pa} procedimentos "
        + "ambulatoriais.",
        num_registros_pa=len(pa),
    )
    logger.debug(
        "Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB.",
        memoria_usada=pa.memory_usage(deep=True).sum() / 10 ** 6,
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
                "regra_contratual_id_cnes",
                "incremento_outros_id_sigtap",
                "incremento_urgencia_id_sigtap",
                "mantenedora_cnpj",
                "receptor_credito_cnpj",
                "financiamento_subtipo_id_sigtap",
                "condicao_principal_id_cid10",
                "autorizacao_id_siasus",
                "profissional_cns",
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
                if all(digito == "9" for digito in elemento)
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
            "servico_especializado_id_cnes",
            function=lambda cod: cod[:3] if pd.notna(cod) else np.nan,
            dest_column_name="servico_id_sigtap",
        )
        .transform_column(
            "servico_especializado_id_cnes",
            function=lambda cod: cod[3:] if pd.notna(cod) else np.nan,
            dest_column_name="servico_classificacao_id_sigtap",
        )
        .remove_columns("servico_especializado_id_cnes")
        # adicionar id
        .add_column("id", str())
        .transform_column("id", function=lambda _: uuid.uuid4().hex)
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


def carregar_pa(
    sessao: Session,
    pa_transformada: pd.DataFrame,
    passo: int = 1000,
) -> int:
    """Carrega um arquivo de disseminação de procedimentos ambulatoriais no BD.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        pa_transformada: [`DataFrame`][] contendo os dados a serem
            carregados na tabela de destino, já no formato utilizado pelo banco
            de dados da ImpulsoGov (conforme retornado pela função
            [`transformar_pa()`][]).
        passo: Indica quantos registros devem ser enviados para a base de dados
            de cada vez.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`transformar_pa()`]: impulsoetl.siasus.procedimentos.transformar_pa
    """

    tabela_nome = tabela_destino.key
    num_registros = len(pa_transformada)
    logger.info(
        "Carregando {num_registros} registros de procedimentos ambulatoriais "
        "para a tabela `{tabela_nome}`...",
        num_registros=num_registros,
        tabela_nome=tabela_nome,
    )

    logger.info("Processando dados para JSON e de volta para um dicionário...")
    registros = json.loads(
        pa_transformada.to_json(
            orient="records",
            date_format="iso",
        )
    )

    conector = sessao.connection()

    # Iterar por fatias do total de registro. Isso é necessário porque
    # executar todas as inserções em uma única operação acarretaria um consumo
    # proibitivo de memória
    contador = 0
    while contador < num_registros:
        logger.info(
            "Enviando registros para a tabela de destino "
            "({contador} de {num_registros})...",
            contador=contador,
            num_registros=num_registros,
        )
        subconjunto_registros = registros[
            contador : min(num_registros, contador + passo)
        ]
        requisicao_insercao = tabela_destino.insert().values(
            subconjunto_registros,
        )
        try:
            conector.execute(requisicao_insercao)
        except Exception as err:
            mensagem_erro = str(err)
            if len(mensagem_erro) > 500:
                mensagem_erro = mensagem_erro[:500]
            logger.error(mensagem_erro)
            breakpoint()
            sessao.rollback()
            return 1

        contador += passo

    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome=tabela_nome,
        linhas_adicionadas=num_registros,
    )

    return 0


def obter_pa(
    sessao: Session,
    uf_sigla: str,
    ano: int,
    mes: int,
    teste: bool = False,
    **kwargs,
) -> None:
    """Baixa, transforma e carrega dados de procedimentos ambulatoriais.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        uf_sigla: Sigla da Unidade Federativa cujos BPA-i's se pretende obter.
        ano: Ano dos procedimentos ambulatoriais que se pretende obter.
        mes: Mês dos procedimentos ambulatoriais que se pretende obter.
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`sqlalchemy.engine.Row`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Row
    """
    logger.info(
        "Iniciando captura de procedimentos ambulatoriais para Unidade "
        + "Federativa '{uf_sigla}' na competencia de {mes}/{ano}.",
        uf_sigla=uf_sigla,
        ano=ano,
        mes=mes,
    )
    logger.info("Fazendo download do FTP público do DataSUS...")
    pa = download(uf_sigla, year=ano, month=mes, group=["PA"])

    # TODO: paralelizar transformação e carregamento de fatias do DataFrame
    # original
    pa_transformada = transformar_pa(sessao=sessao, pa=pa)

    if teste:
        passo = 10
        pa_transformada = pa_transformada.iloc[
            : min(1000, len(pa_transformada)),
        ]
        if len(pa_transformada) == 1000:
            logger.warning(
                "Arquivo de procedimentos ambulatoriais truncado para 1000 "
                + "registros para fins de teste."
            )
    else:
        passo = 1000

    carregar_pa(
        sessao=sessao,
        pa_transformada=pa_transformada,
        passo=passo,
    )
    if not teste:
        sessao.commit()
