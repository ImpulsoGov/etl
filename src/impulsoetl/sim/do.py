# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém dados dos arquivos de disseminação das Declarações de Óbito (DOs)."""


from __future__ import annotations

import os
import re
from datetime import date
from typing import Final, Generator

import janitor  # noqa: F401  # nopycln: import
import numpy as np
import pandas as pd
from frozendict import frozendict
from sqlalchemy.orm import Session
from uuid6 import uuid7

from impulsoetl.comum.condicoes_saude import e_cid10, remover_ponto_cid10
from impulsoetl.comum.datas import agora_gmt_menos3
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import logger
from impulsoetl.utilitarios.bd import carregar_dataframe
from impulsoetl.utilitarios.datasus_ftp import extrair_dbc_lotes


DE_PARA_DO: Final[frozendict] = frozendict(
    {
        "TIPOBITO": "tipo_id_sim",
        "DTOBITO": "ocorrencia_data",
        "NATURAL": "usuario_nascimento_pais_uf_id_sus",
        "DTNASC": "usuario_nascimento_data",
        "IDADE": "usuario_idade_id_sim",
        "SEXO": "usuario_sexo_id_sim",
        "RACACOR": "usuario_raca_cor_id_sim",
        "ESTCIV": "usuario_estado_civil_id_sim",
        "ESC": "usuario_escolaridade_id_sim1996",
        "OCUP": "usuario_ocupacao_id_cbo2002",
        "CODMUNRES": "usuario_residencia_municipio_id_sus",
        "LOCOCOR": "local_ocorrencia_id_sim",
        "CODMUNOCOR": "unidade_geografica_id_sus",
        "IDADEMAE": "mae_idade",
        "ESCMAE": "mae_escolaridade_id_sim1996",
        "OCUPMAE": "mae_ocupacao_id_cbo2002",
        "QTDFILVIVO": "mae_filhos_nascidos_vivos",
        "QTDFILMORT": "mae_filhos_perdas_fetais",
        "GRAVIDEZ": "gestacao_tipo_id_sim",
        "GESTACAO": "gestacao_semanas_id_sim",
        "PARTO": "parto_tipo_id_sim",
        "OBITOPARTO": "parto_relacao_id_sim",
        "PESO": "usuario_nascimento_peso",
        "OBITOGRAV": "gestacao_relacao",
        "OBITOPUERP": "puerperio_relacao",
        "ASSISTMED": "assistencia_medica_recebeu",
        "EXAME": "exame_realizou",
        "CIRURGIA": "cirurgia_realizou",
        "NECROPSIA": "necropsia_realizou",
        "LINHAA": "condicoes_terminais_ids_cid10",
        "LINHAB": "condicoes_antecedentes_consequenciais_1_ids_cid10",
        "LINHAC": "condicoes_antecedentes_consequenciais_2_ids_cid10",
        "LINHAD": "condicoes_basicas_ids_cid10",
        "LINHAII": "condicoes_contribuintes_ids_cid10",
        "CAUSABAS": "causa_basica_resselecao_apos_id_cid10",
        "CIRCOBITO": "circunstancia_id_sim",
        "ACIDTRAB": "acidente_trabalho",
        "FONTE": "circunstancia_fonte_id_sim",
    },
)

DE_PARA_DO_ADICIONAIS: Final[frozendict] = frozendict({
    "ORIGEM": "origem_id_sim",
    "HORAOBITO": "ocorrencia_hora",
    "CODMUNNATU": "usuario_nascimento_municipio_id_sus",
    "ESC2010": "usuario_escolaridade_id_sim2010",
    "SERIESCFAL": "usuario_escolaridade_serie",
    "CODESTAB": "estabelecimento_id_scnes",
    "ESTABDESCR": "_nao_documentado_estabdescr",
    "ESCMAE2010": "mae_escolaridade_id_sim2010",
    "SERIESCMAE": "mae_escolaridade_serie",
    "SEMAGESTAC": "gestacao_semanas",
    "TPMORTEOCO": "gestacao_situacao_id_sim2012",
    "CB_PRE": "causa_basica_resselecao_antes_localidade_id_cid10",
    "COMUNSVOIM": "svo_iml_municipio_id_sus",
    "DTATESTADO": "atestado_data",
    "NUMEROLOTE": "lote_id_sim",
    "TPPOS": "investigacao_houve",
    "DTINVESTIG": "investigacao_data",
    "CAUSABAS_O": "causa_basica_resselecao_antes_id_cid10",
    "DTCADASTRO": "cadastro_data",
    "ATESTANTE": "atestado_atestante_tipo_id_sim",
    "STCODIFICA": "sistema_instalacao_codificadora",
    "CODIFICADO": "declaracao_codificada",
    "VERSAOSIST": "sistema_versao",
    "VERSAOSCB": "causa_basica_seletor_versao",
    "FONTEINV": "investigacao_fonte_id_sim",
    "DTRECEBIM": "recebimento_data",
    "ATESTADO": "atestado_condicoes_ids_cid10",
    "DTRECORIGA": "recebimento_original_data",
    "CAUSAMAT": "causa_externa_id_cid10",
    "ESCMAEAGR1": "mae_escolaridade_agregada_id_sim",
    "ESCFALAGR1": "usuario_escolaridade_agregada_id_sim",
    "STDOEPIDEM": "declaracao_modelo_epidemiologica",
    "STDONOVA": "declaracao_modelo_novo",
    "DIFDATA": "recebimento_original_intervalo",
    "NUDIASOBCO": "investigacao_duracao",
    "NUDIASOBIN": "_nao_documentado_nudiasobin",
    "DTCADINV": "investigacao_cadastro_data",
    "TPOBITOCOR": "gestacao_situacao_id_sim2009",
    "DTCONINV": "investigacao_conclusao_data",
    "FONTES": "fontes_combinacao_id_sim",
    "TPRESGINFO": "investigacao_desfecho_id_sim",
    "TPNIVELINV": "investigacao_esfera_id_sim",
    "NUDIASINF": "_nao_documentado_nudiasinf",
    "DTCADINF": "_nao_documentado_dtcadinf",
    "MORTEPARTO": "_nao_documentado_morteparto",
    "DTCONCASO": "conclusao_data",
    "FONTESINF": "_nao_documentado_fontesinf",
    "ALTCAUSA": "investigacao_gerou_alteracao",
    "CONTADOR": "_nao_documentado_contador",
    "CRM": "atestado_atestante_id_crm",
    "CODBAIRES": "usuario_residencia_bairro_id_sim",
    "UFINFORM": "uf_id_ibge",
    "CODBAIOCOR": "ocorrencia_bairro_id_sim",
    "TPASSINA": "_nao_documentado_tpassina",
})

TIPOS_DO: Final[frozendict] = frozendict(
    {
        "id": "object",
        "periodo_id": "object",
        "unidade_geografica_id": "object",
        "tipo_id_sim": "object",
        "ocorrencia_data": "datetime64[ns]",
        "usuario_nascimento_pais_uf_id_sus": "object",
        "usuario_nascimento_data": "datetime64[ns]",
        "usuario_idade_id_sim": "object",
        "usuario_sexo_id_sim": "object",
        "usuario_raca_cor_id_sim": "object",
        "usuario_estado_civil_id_sim": "object",
        "usuario_escolaridade_id_sim1996": "object",
        "usuario_ocupacao_id_cbo2002": "object",
        "usuario_residencia_municipio_id_sus": "object",
        "local_ocorrencia_id_sim": "object",
        "unidade_geografica_id_sus": "object",
        "mae_idade": "Int64",
        "mae_escolaridade_id_sim1996": "object",
        "mae_ocupacao_id_cbo2002": "object",
        "mae_filhos_nascidos_vivos": "Int64",
        "mae_filhos_perdas_fetais": "Int64",
        "gestacao_tipo_id_sim": "object",
        "gestacao_semanas_id_sim": "object",
        "parto_tipo_id_sim": "object",
        "parto_relacao_id_sim": "object",
        "usuario_nascimento_peso": "Int64",
        "gestacao_relacao": "boolean",
        "puerperio_relacao": "boolean",
        "assistencia_medica_recebeu": "boolean",
        "exame_realizou": "boolean",
        "cirurgia_realizou": "boolean",
        "necropsia_realizou": "boolean",
        "condicoes_terminais_ids_cid10": "object",  # array
        "condicoes_antecedentes_consequenciais_1_ids_cid10": "object",  # array
        "condicoes_antecedentes_consequenciais_2_ids_cid10": "object",  # array
        "condicoes_basicas_ids_cid10": "object",  # array
        "condicoes_contribuintes_ids_cid10": "object",  # array
        "causa_basica_resselecao_apos_id_cid10": "object",
        "circunstancia_id_sim": "object",
        "acidente_trabalho": "boolean",
        "circunstancia_fonte_id_sim": "object",
        "criacao_data": "datetime64[ns]",
        "atualizacao_data": "datetime64[ns]",
    },
)

TIPOS_DO_ADICIONAIS: Final(frozendict) = frozendict({
    "origem_id_sim": "object",
    "ocorrencia_hora": "object",  # pandas não tem tipo apropriado p/ hora
    "atestado_atestante_id_crm": "object",
    "usuario_nascimento_municipio_id_sus": "object",
    "usuario_escolaridade_id_sim2010": "object",
    "usuario_escolaridade_serie": "object",
    "estabelecimento_id_scnes": "object",
    "_nao_documentado_estabdescr": "object",
    "mae_escolaridade_id_sim2010": "object",
    "mae_escolaridade_serie": "object",
    "gestacao_semanas": "Int64",
    "gestacao_situacao_id_sim2012": "object",
    "causa_basica_resselecao_antes_localidade_id_cid10": "object",
    "svo_iml_municipio_id_sus": "object",
    "atestado_data": "datetime64[ns]",
    "lote_id_sim": "object",
    "investigacao_houve": "boolean",
    "investigacao_data": "datetime64[ns]",
    "causa_basica_resselecao_antes_id_cid10": "object",
    "cadastro_data": "datetime64[ns]",
    "atestado_atestante_tipo_id_sim": "object",
    "sistema_instalacao_codificadora": "boolean",
    "declaracao_codificada": "boolean",
    "sistema_versao": "object",
    "causa_basica_seletor_versao": "object",
    "investigacao_fonte_id_sim": "object",
    "recebimento_data": "datetime64[ns]",
    "atestado_condicoes_ids_cid10": "object",
    "recebimento_original_data": "datetime64[ns]",
    "causa_externa_id_cid10": "object",
    "mae_escolaridade_agregada_id_sim": "object",
    "usuario_escolaridade_agregada_id_sim": "object",
    "declaracao_modelo_epidemiologica": "boolean",
    "declaracao_modelo_novo": "boolean",
    "recebimento_original_intervalo": "object",  # intervalo
    "investigacao_duracao": "object",  # intervalo
    "_nao_documentado_nudiasobin": "object",
    "investigacao_cadastro_data": "datetime64[ns]",
    "gestacao_situacao_id_sim2009": "object",
    "investigacao_conclusao_data": "datetime64[ns]",
    "fontes_combinacao_id_sim": "object",
    "investigacao_desfecho_id_sim": "object",
    "investigacao_esfera_id_sim": "object",
    "_nao_documentado_nudiasinf": "object",
    "_nao_documentado_dtcadinf": "object",
    "_nao_documentado_morteparto": "object",
    "conclusao_data": "datetime64[ns]",
    "_nao_documentado_fontesinf": "object",
    "investigacao_gerou_alteracao": "boolean",
    "_nao_documentado_contador": "object",
    "usuario_residencia_bairro_id_sim": "object",
    "uf_id_ibge": "object",
    "ocorrencia_bairro_id_sim": "object",
    "_nao_documentado_tpassina": "object",
})

COLUNAS_DATA_DDMMAAAA: Final[list[str]] = [
    "ocorrencia_data",
    "usuario_nascimento_data",
    "atestado_data",
    "investigacao_data",
    "cadastro_data",
    "recebimento_data",
    "recebimento_original_data",
    "investigacao_cadastro_data",
    "investigacao_conclusao_data",
    "conclusao_data",
]

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_DO.items()
    if tipo_coluna.lower() == "Int64" or tipo_coluna.lower() == "float64"
]

COLUNAS_INTERVALOS: Final[list[str]] = [
    "recebimento_original_intervalo",
    "investigacao_duracao",
]


def _para_booleano(valor: str) -> bool | float:
    """Transforma um valor binário '0' ou '1' em booleano. Suporta NaNs."""
    if valor == "1":
        return True
    elif valor == "2":
        return False
    else:
        return np.nan


def extrair_do(
    uf_sigla: str,
    periodo_data_inicio: date,
    passo: int = 10000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai registros de Declarações de Óbito do FTP do DataSUS.

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
        caminho_diretorio="/dissemin/publicos/SIM/CID10/DORES/",
        arquivo_nome="DO{uf_sigla}{periodo_data_inicio:%Y}.dbc".format(
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        ),
        passo=passo,
    )


def transformar_do(
    sessao: Session,
    do: pd.DataFrame,
    periodo_id: str,
    condicoes: str | None = None,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de Declarações de Óbito obtidos do DataSUS.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        do: objeto [`pandas.DataFrame`][] contendo os dados de um arquivo de
            disseminação de Declarações de Óbito, conforme extraídos para uma
            unidade federativa e competência (mês) pela função [`extrair_do()`][].
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
        público do DataSUS. Verifique o [Informe Técnico][it-sim] para mais
        informações.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`extrair_do()`]: impulsoetl.sim.do.extrair_do
    [`pandas.DataFrame.query()`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
    [estrutura-sim]: https://drive.google.com/file/d/1CoRX_l-h7weaRv16RHDa_4wenrZW6jD5/view?usp=sharing
    """
    logger.info(
        "Transformando DataFrame com {num_registros_do} Declarações de Óbito.",
        num_registros_do=len(do),
    )
    logger.debug(
        "Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB.",
        memoria_usada=do.memory_usage(deep=True).sum() / 10 ** 6,
    )

    # aplica condições de filtragem dos registros
    if condicoes:
        do = do.query(condicoes, engine="python")
        logger.info(
            "Registros após aplicar condições de filtragem: {num_registros}.",
            num_registros=len(do),
        )

    # Junta nomes de colunas e tipos adicionais aos obrigatórios
    de_para = dict(DE_PARA_DO, **DE_PARA_DO_ADICIONAIS)
    tipos = dict(TIPOS_DO, **TIPOS_DO_ADICIONAIS)

    # corrigir nomes de colunas mal formatados
    do = do.rename_columns(function=lambda col: col.strip().upper())

    do_transformada = (
        do  # noqa: WPS221  # ignorar linha complexa no pipeline
        # adicionar colunas faltantes, com valores vazios
        .add_columns(**{
            coluna: ""
            for coluna in DE_PARA_DO_ADICIONAIS.keys()
            if not (coluna in do.columns)
        })
        # renomear colunas
        .rename_columns(de_para)
        # processar colunas com datas
        .transform_columns(
            # corrigir datas com dígito 0 substituído por espaço
            COLUNAS_DATA_DDMMAAAA + ["ocorrencia_hora"],
            function=lambda dt: dt.replace(" ", "0"),
        )
        .transform_columns(
            COLUNAS_DATA_DDMMAAAA,
            function=lambda dt: pd.to_datetime(
                dt,
                format="%d%m%Y",  # noqa: WPS323
                errors="coerce",
            ),
        )
        .transform_column(
            "ocorrencia_hora",
            function=lambda hora: (
                # TODO: Corrigir hora > 24
                hora[:2] + ":" + hora[2:4]
                if re.match(r"([01][0-9]|2[0-3])[0-5][0-9]", hora)
                else np.nan
            ),
        )
        # processar colunas com intervalos
        .transform_columns(
            COLUNAS_INTERVALOS,
            function=lambda intervalo: (
                str(int(intervalo)) + " days" if intervalo else np.nan
            ),
        )
        # processar colunas lógicas
        .transform_column(
            "sistema_instalacao_codificadora",
            function=lambda elemento: True if elemento == "S" else False,
        )
        .transform_columns(
            [
                "declaracao_modelo_epidemiologica",
                "declaracao_modelo_novo",
            ],
            function=lambda elemento: True if elemento == "1" else False,
        )
        .transform_columns(
            [
                "gestacao_relacao",
                "puerperio_relacao",
                "assistencia_medica_recebeu",
                "exame_realizou",
                "cirurgia_realizou",
                "necropsia_realizou",
                "acidente_trabalho",
                "investigacao_houve",
                "declaracao_codificada",
                "investigacao_gerou_alteracao",
            ],
            function=_para_booleano,
        )
        # processar colunas com CIDs
        .transform_columns(
            [
                "condicoes_terminais_ids_cid10",
                "condicoes_antecedentes_consequenciais_1_ids_cid10",
                "condicoes_antecedentes_consequenciais_2_ids_cid10",
                "condicoes_basicas_ids_cid10",
                "condicoes_contribuintes_ids_cid10",
                "causa_basica_resselecao_apos_id_cid10",
                "causa_basica_resselecao_antes_localidade_id_cid10",
                "causa_externa_id_cid10",
            ],
            function=lambda cids: cids.strip("*/ ")
        )
        .transform_columns(
            [
                "condicoes_terminais_ids_cid10",
                "condicoes_antecedentes_consequenciais_1_ids_cid10",
                "condicoes_antecedentes_consequenciais_2_ids_cid10",
                "condicoes_basicas_ids_cid10",
                "condicoes_contribuintes_ids_cid10",
            ],
            function=remover_ponto_cid10,
        )
        .transform_columns(
            [
                "condicoes_terminais_ids_cid10",
                "condicoes_antecedentes_consequenciais_1_ids_cid10",
                "condicoes_antecedentes_consequenciais_2_ids_cid10",
                "condicoes_basicas_ids_cid10",
                "condicoes_contribuintes_ids_cid10",
            ],
            function=lambda cids: (
                "{"
                + ",".join([
                    cid for cid in re.split("[^a-zA-Z0-9]", cids)
                    if len(cid) > 0  # remove campos com CIDs vazios
                    and e_cid10(cid)  # remove texto que não é um CID10 válido
                ])
                + "}"
            ),
        )
        # Processar identificadores que podem ser IBGE ou SUS - antes de 2008,
        # alguns desses campos utilizavam identificadores de municípios do
        # IBGE (7 dígitos); depois passaram a usar identificadores SUS (6 
        # dígitos).
        .transform_columns(
            [
                "svo_iml_municipio_id_sus",
                "unidade_geografica_id_sus",
                "usuario_nascimento_municipio_id_sus",
                "usuario_residencia_municipio_id_sus",
            ],
            function=lambda id_ibge_ou_sus: (
                id_ibge_ou_sus[0:min(6, len(id_ibge_ou_sus))]
                if id_ibge_ou_sus
                else np.nan
            ),
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
        .transform_columns(
            [
                "origem_id_sim",
                "tipo_id_sim",
                "ocorrencia_hora",
                "usuario_nascimento_pais_uf_id_sus",
                "usuario_nascimento_municipio_id_sus",
                "usuario_sexo_id_sim",
                "usuario_raca_cor_id_sim",
                "usuario_estado_civil_id_sim",
                "usuario_escolaridade_id_sim1996",
                "usuario_escolaridade_serie",
                "usuario_ocupacao_id_cbo2002",
                "usuario_residencia_municipio_id_sus",
                "local_ocorrencia_id_sim",
                "estabelecimento_id_scnes",
                "_nao_documentado_estabdescr",
                "unidade_geografica_id_sus",
                "mae_escolaridade_id_sim1996",
                "mae_escolaridade_serie",
                "mae_ocupacao_id_cbo2002",
                "gestacao_tipo_id_sim",
                "gestacao_semanas_id_sim",
                "parto_tipo_id_sim",
                "parto_relacao_id_sim",
                "gestacao_situacao_id_sim2012",
                "causa_basica_resselecao_apos_id_cid10",
                "causa_basica_resselecao_antes_localidade_id_cid10",
                "svo_iml_municipio_id_sus",
                "circunstancia_id_sim",
                "circunstancia_fonte_id_sim",
                "lote_id_sim",
                "causa_basica_resselecao_antes_id_cid10",
                "atestado_atestante_tipo_id_sim",
                "sistema_versao",
                "causa_basica_seletor_versao",
                "investigacao_fonte_id_sim",
                "atestado_condicoes_ids_cid10",
                "causa_externa_id_cid10",
                "gestacao_situacao_id_sim2009",
                "fontes_combinacao_id_sim",
                "investigacao_desfecho_id_sim",
                "investigacao_esfera_id_sim",
            ],
            function=lambda elemento: (
                np.nan
                if (
                    pd.notna(elemento)
                    and all(digito == "0" for digito in elemento)
                )
                else elemento
            ),
        )
        # adicionar id
        .add_column("id", str())
        .transform_column("id", function=lambda _: uuid7().hex)
        # adicionar id do periodo
        .assign(periodo_id=periodo_id)
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
        .astype(tipos)
    )

    logger.debug(
        "Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB.",
        memoria_usada=(
            do_transformada.memory_usage(deep=True).sum() / 10 ** 6
        ),
    )
    return do_transformada


def obter_do(
    sessao: Session,
    uf_sigla: str,
    periodo_id: str,
    periodo_data_inicio: date,
    tabela_destino: str,
    teste: bool = False,
    **kwargs,
) -> None:
    """Baixa, transforma e carrega dados de Declarações de Óbitos do DataSUS.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        uf_sigla: Sigla da Unidade Federativa cujas Declarações de Óbito se
            pretende obter.
        periodo_id: Identificador único do período de referência da Declaração
            de Óbito no banco de dados da Impulso Gov.
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
            [`transformar_do()`][].

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`sqlalchemy.engine.Row`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Row
    [`datetime.date`]: https://docs.python.org/3/library/datetime.html#date-objects
    [`transformar_do()`]: impulsoetl.sim.do.transformar_do
    """
    logger.info(
        "Iniciando captura de Declarações de Óbito para Unidade Federativa "
        + "Federativa '{}' na competencia de {:%m/%Y}.",
        uf_sigla,
        periodo_data_inicio,
    )

    # obter tamanho do lote de processamento
    passo = int(os.getenv("IMPULSOETL_LOTE_TAMANHO", 100000))

    do_lotes = extrair_do(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
        passo=passo,
    )

    contador = 0
    for do_lote in do_lotes:
        do_transformada = transformar_do(
            sessao=sessao,
            do=do_lote,
            periodo_id=periodo_id,
            condicoes=kwargs.get("condicoes"),
        )

        carregamento_status = carregar_dataframe(
            sessao=sessao,
            df=do_transformada,
            tabela_destino=tabela_destino,
            passo=None,
            teste=teste,
        )
        if carregamento_status != 0:
            raise RuntimeError(
                "Execução interrompida em razão de um erro no "
                + "carregamento."
            )
        contador += len(do_lote)
        if teste and contador > 1000:
            logger.info("Execução interrompida para fins de teste.")
            break

    if teste:
        logger.info("Desfazendo alterações realizadas durante o teste...")
        sessao.rollback()
        logger.info("Todas transações foram desfeitas com sucesso!")
