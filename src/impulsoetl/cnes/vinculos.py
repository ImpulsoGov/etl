# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Ferramentas para obter dados de vínculos profissionais registrados no CNES."""


from __future__ import annotations

import json
import re
import uuid
from typing import Final

import janitor  # noqa: F401  # nopycln: import
import numpy as np
import pandas as pd
import roman
from frozendict import frozendict
from pysus.online_data.CNES import download
from sqlalchemy.orm import Session

from impulsoetl.cnes.modelos import vinculos as tabela_destino
from impulsoetl.comum.datas import periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import logger

DE_PARA_VINCULOS: Final[frozendict] = frozendict(
    {
        "CNES": "estabelecimento_id_cnes",
        "CODUFMUN": "estabelecimento_municipio_id_sus",
        "REGSAUDE": "estabelecimento_regiao_saude_id_sus",
        "MICR_REG": "estabelecimento_microrregiao_saude_id_sus",
        "DISTRSAN": "estabelecimento_distrito_sanitario_id_sus",
        "DISTRADM": "estabelecimento_distrito_administrativo_id_sus",
        "TPGESTAO": "estabelecimento_gestao_condicao_id_cnes",
        "PF_PJ": "estabelecimento_personalidade_juridica_id_cnes",
        "CPF_CNPJ": "estabelecimento_cpf_cnpj",
        "NIV_DEP": "estabelecimento_mantido",
        "CNPJ_MAN": "estabelecimento_mantenedora_cnpj",
        "ESFERA_A": "estabelecimento_esfera_administrativa_id_cnes",
        "ATIVIDAD": "estabelecimento_atividade_ensino_id_cnes",
        "RETENCAO": "estabelecimento_tributos_retencao_id_cnes",
        "NATUREZA": "estabelecimento_natureza_id_cnes",
        "CLIENTEL": "estabelecimento_tipo_id_cnes",
        "TP_UNID": "estabelecimento_fluxo_id_cnes",
        "TURNO_AT": "estabelecimento_turno_id_cnes",
        "NIV_HIER": "estabelecimento_hierarquia_id_cnes",
        "TERCEIRO": "estabelecimento_terceiro",
        "CPF_PROF": "profissional_id_cpf_criptografado",
        "CPFUNICO": "profissional_cpf_unico",
        "CBO": "ocupacao_id_cbo",
        "CBOUNICO": "ocupacao_cbo_unico",
        "NOMEPROF": "profissional_nome",
        "CNS_PROF": "profissional_id_cns",
        "CONSELHO": "profissional_conselho_tipo_id_cnes",
        "REGISTRO": "profissional_id_conselho",
        "VINCULAC": "tipo_id_cnes",
        "VINCUL_C": "contratado",
        "VINCUL_A": "autonomo",
        "VINCUL_N": "sem_vinculo_definido",
        "PROF_SUS": "atendimento_sus",
        "PROFNSUS": "atendimento_nao_sus",
        "HORAOUTR": "atendimento_carga_outras",
        "HORAHOSP": "atendimento_carga_hospitalar",
        "HORA_AMB": "atendimento_carga_ambulatorial",
        "COMPETEN": "periodo_data_inicio",
        "UFMUNRES": "profissional_residencia_municipio_id_sus",
        "NAT_JUR": "estabelecimento_natureza_juridica_id_cnes",
    },
)

TIPOS_VINCULOS: Final[frozendict] = frozendict(
    {
        "id": "object",
        "unidade_geografica_id": "object",
        "periodo_id": "object",
        "estabelecimento_id_cnes": "object",
        "estabelecimento_municipio_id_sus": "object",
        "estabelecimento_regiao_saude_id_sus": "object",
        "estabelecimento_microrregiao_saude_id_sus": "object",
        "estabelecimento_distrito_sanitario_id_sus": "object",
        "estabelecimento_distrito_administrativo_id_sus": "object",
        "estabelecimento_gestao_condicao_id_cnes": "object",
        "estabelecimento_personalidade_juridica_id_cnes": "object",
        "estabelecimento_cpf_cnpj": "object",
        "estabelecimento_mantido": "boolean",
        "estabelecimento_mantenedora_cnpj": "object",
        "estabelecimento_esfera_administrativa_id_cnes": "object",
        "estabelecimento_atividade_ensino_id_cnes": "object",
        "estabelecimento_tributos_retencao_id_cnes": "object",
        "estabelecimento_natureza_id_cnes": "object",
        "estabelecimento_tipo_id_cnes": "object",
        "estabelecimento_fluxo_id_cnes": "object",
        "estabelecimento_turno_id_cnes": "object",
        "estabelecimento_hierarquia_id_cnes": "object",
        "estabelecimento_terceiro": "boolean",
        "profissional_id_cpf_criptografado": "object",
        "profissional_cpf_unico": "object",
        "ocupacao_id_cbo": "object",
        "ocupacao_cbo_unico": "object",
        "profissional_nome": "object",
        "profissional_id_cns": "object",
        "profissional_conselho_tipo_id_cnes": "object",
        "profissional_id_conselho": "object",
        "tipo_id_cnes": "object",
        "contratado": "boolean",
        "autonomo": "boolean",
        "sem_vinculo_definido": "boolean",
        "atendimento_sus": "boolean",
        "atendimento_nao_sus": "boolean",
        "atendimento_carga_outras": "int64",
        "atendimento_carga_hospitalar": "int64",
        "atendimento_carga_ambulatorial": "int64",
        "periodo_data_inicio": "datetime64[ns]",
        "profissional_residencia_municipio_id_sus": "object",
        "estabelecimento_natureza_juridica_id_cnes": "object",
    },
)

COLUNAS_DATA_AAAAMM: Final[list[str]] = [
    "periodo_data_inicio",
]

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_VINCULOS.items()
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


def _romano_para_inteiro(texto: str) -> str | float:
    if pd.isna(texto):
        return np.nan
    try:
        return str(roman.fromRoman(texto))
    except roman.InvalidRomanNumeralError:
        return texto


def transformar_vinculos(
    sessao: Session,
    vinculos: pd.DataFrame,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de vínculos do CNES.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        vinculos: [`DataFrame`][] contendo os dados a serem transformados
            (conforme retornado pela função
            [`pysus.online_data.CNES.download()`][] com o argumento
            `group='PF'`).

    Retorna:
        Um [`DataFrame`][] com dados de vínculos profissionais tratados para
        inserção no banco de dados da ImpulsoGov.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`pysus.online_data.CNES.download()`]: http://localhost:9090/@https://github.com/AlertaDengue/PySUS/blob/600c61627b7998a1733b71ac163b3de71324cfbe/pysus/online_data/CNES.py#L28
    """
    logger.info(
        "Transformando DataFrame com {num_registros} vínculos "
        + "profissionais do CNES.",
        num_registros=len(vinculos),
    )
    logger.debug(
        "Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB.",
        memoria_usada=vinculos.memory_usage(deep=True).sum() / 10 ** 6,
    )
    vinculos_transformado = (
        vinculos  # noqa: WPS221  # ignorar linha complexa no pipeline
        # renomear colunas
        .rename_columns(function=lambda col: col.strip())
        .rename_columns(DE_PARA_VINCULOS)
        # processar colunas com datas
        .transform_columns(
            COLUNAS_DATA_AAAAMM,
            function=lambda dt: pd.to_datetime(
                dt,
                format="%Y%m",
                errors="coerce",
            ),
        )
        # limpar e completar códigos de região e distrito de saúde
        .transform_column(
            "estabelecimento_regiao_saude_id_sus",
            _romano_para_inteiro,
        )
        .transform_column(
            "estabelecimento_regiao_saude_id_sus",
            lambda id_sus: (
                re.sub("[^0-9]", "", id_sus) if pd.notna(id_sus) else np.nan
            ),
        )
        .transform_columns(
            [
                "estabelecimento_regiao_saude_id_sus",
                "estabelecimento_distrito_sanitario_id_sus",
                "estabelecimento_distrito_administrativo_id_sus",
            ],
            lambda id_sus: (id_sus.zfill(4) if pd.notna(id_sus) else np.nan),
        )
        .transform_column(
            "estabelecimento_microrregiao_saude_id_sus",
            lambda id_sus: (id_sus.zfill(6) if pd.notna(id_sus) else np.nan),
        )
        # limpar registros no conselho profissional
        .transform_column(
            "profissional_id_conselho",
            lambda id_conselho: (
                re.sub("[^0-9]", "", id_conselho)
                if pd.notna(id_conselho)
                else np.nan
            ),
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
        .transform_columns(
            [
                "estabelecimento_regiao_saude_id_sus",
                "estabelecimento_microrregiao_saude_id_sus",
                "estabelecimento_distrito_sanitario_id_sus",
                "estabelecimento_distrito_administrativo_id_sus",
                "estabelecimento_cpf_cnpj",
                "estabelecimento_mantenedora_cnpj",
                "profissional_id_conselho",
                "profissional_residencia_municipio_id_sus",
            ],
            function=lambda elemento: (
                np.nan
                if pd.notna(elemento)
                and all(digito == "0" for digito in elemento)
                else elemento
            ),
        )
        # processar colunas lógicas
        .transform_column(
            "estabelecimento_mantido",
            function=lambda elemento: True if elemento == "1" else False,
        )
        .transform_columns(
            [
                "estabelecimento_terceiro",
                "contratado",
                "autonomo",
                "sem_vinculo_definido",
                "atendimento_sus",
                "atendimento_nao_sus",
            ],
            function=_para_booleano,
        )
        # adicionar id
        .add_column("id", str())
        .transform_column("id", function=lambda _: uuid.uuid4().hex)
        # adicionar id do periodo
        .transform_column(
            "periodo_data_inicio",
            function=lambda dt: periodo_por_data(sessao=sessao, data=dt).id,
            dest_column_name="periodo_id",
        )
        # adicionar id da unidade geografica
        .transform_column(
            "estabelecimento_municipio_id_sus",
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
        .astype(TIPOS_VINCULOS)
    )
    logger.debug(
        "Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB.",
        memoria_usada=(
            vinculos_transformado.memory_usage(deep=True).sum() / 10 ** 6
        ),
    )
    return vinculos_transformado


def carregar_vinculos(
    sessao: Session,
    vinculos_transformado: pd.DataFrame,
    passo: int = 1000,
) -> int:
    """Carrega um arquivo de disseminação de vínculos profissionais no BD.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        vinculos_transformado: [`DataFrame`][] contendo os dados a serem
            carregados na tabela de destino, já no formato utilizado pelo banco
            de dados da ImpulsoGov (conforme retornado pela função
            [`transformar_vinculos()`][]).
        passo: Indica quantos registros devem ser enviados para a base de dados
            de cada vez.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`transformar_vinculos()`]: impulsoetl.cnes.vinculos.transformar_vinculos
    """

    tabela_nome = tabela_destino.key
    num_registros = len(vinculos_transformado)
    logger.info(
        "Carregando {num_registros} registros de vinculos profissionais "
        "para a tabela `{tabela_nome}`...",
        num_registros=num_registros,
        tabela_nome=tabela_nome,
    )

    logger.info("Processando dados para JSON e de volta para um dicionário...")
    registros = json.loads(
        vinculos_transformado.to_json(
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


def obter_vinculos(
    sessao: Session,
    uf_sigla: str,
    ano: int,
    mes: int,
    teste: bool = False,
    **kwargs,
) -> None:
    """Baixa, transforma e carrega dados de vinculos profissionais.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        uf_sigla: Sigla da Unidade Federativa cujos BPA-i's se pretende obter.
        ano: Ano dos vínculos profissionais que se pretende obter.
        mes: Mês dos vínculos profissionais que se pretende obter.
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`sqlalchemy.engine.Row`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Row
    """
    logger.info(
        "Iniciando captura de vínculos profissionais para Unidade "
        + "Federativa '{uf_sigla}' na competencia de {mes}/{ano}.",
        uf_sigla=uf_sigla,
        ano=ano,
        mes=mes,
    )
    logger.info("Fazendo download do FTP público do DataSUS...")
    vinculos = download(state=uf_sigla, year=ano, month=mes, group="PF")

    # TODO: paralelizar transformação e carregamento de fatias do DataFrame
    # original
    vinculos_transformado = transformar_vinculos(
        sessao=sessao,
        vinculos=vinculos,
    )

    if teste:
        passo = 10
        vinculos_transformado = vinculos_transformado.iloc[
            : min(1000, len(vinculos_transformado)),
        ]
        if len(vinculos_transformado) == 1000:
            logger.warning(
                "Arquivo de vínculos profissionais truncado para 1000 "
                + "registros para fins de teste."
            )
    else:
        passo = 1000

    carregar_vinculos(
        sessao=sessao,
        vinculos_transformado=vinculos_transformado,
        passo=passo,
    )
    if not teste:
        sessao.commit()
