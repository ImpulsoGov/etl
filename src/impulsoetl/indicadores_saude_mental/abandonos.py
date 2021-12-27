# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém indicadores sobre o abandono de usuários recentes em CAPS."""


from __future__ import annotations

from datetime import datetime

import janitor  # noqa: F401  # nopycln: import
import numpy as np
import pandas as pd
import pandas_flavor as pf
from sqlalchemy.orm import Session

from impulsoetl.indicadores_saude_mental.comum import consultar_raas
from impulsoetl.loggers import logger

colunas_a_agrupar = [
    "competencia_realizacao",
    "servico_descricao",
    "servico_classificacao_descricao",
    "estabelecimento_tipo_descricao",
    "estabelecimento_nome",
    "estabelecimento_latitude",
    "estabelecimento_longitude",
    "encaminhamento_origem_descricao",
    "usuario_sexo",
    "usuario_raca_cor",
    "usuario_faixa_etaria",
    "usuario_situacao_rua",
    "usuario_substancias_abusa",
    "usuario_estabelecimento_referencia_nome",
    "usuario_estabelecimento_referencia_latitude",
    "usuario_estabelecimento_referencia_longitude",
    "usuario_tempo_no_servico",
    "usuario_novo",
    "cid_descricao",
    "cid_grupo_descricao_curta",
]


@pf.register_dataframe_method
def maxima_movel_retroativa(
    df: pd.DataFrame,
    coluna_nome: str,
    agrupar_por: list,
    coluna_destino: str,
    janela: int,
) -> pd.DataFrame:
    df[coluna_destino] = df.groupby(agrupar_por)[coluna_nome].transform(
        lambda s: (
            s.rolling(janela, min_periods=1)
            .max()
            # desloca os períodos retroativamente
            # ver: https://stackoverflow.com/a/38056328/7733563
            .shift(periods=1 - janela)
            .astype(bool)
        ),
    )
    return df


def caracterizar_abandonos(
    raas: pd.DataFrame,
    tempo_no_caps: int = 6,
    intervalo_inatividade: int = 3,
    remover_perfil_ambulatorial: bool = True,
) -> pd.DataFrame:

    atendimento_individual = (
        [
            "Atendimento individual de paciente em centro de atenção "
            + "psicossocial"
        ]
        if remover_perfil_ambulatorial
        # lista vazia para não encontrar nenhum procedimento na hora de remover
        # os usuários que realizaram apenas atendimentos ambulatoriais,
        # caso se deseje preservar esses usuários na conta
        else []
    )

    # as competências mais antigas não devem ser utilizadas, já que parte dos
    # usuários recém-chegados deram entrada antes da implementação da RAAS
    data_minima = pd.Timestamp(2013, 1, 1).tz_localize(  # noqa: F841
        "Etc/GMT+3"
    ) + pd.DateOffset(months=tempo_no_caps)

    # as competências mais recentes não devem ser utilizadas porque ainda não houve
    # tempo de determinar se o usuário entrou em inatividade
    data_maxima = max(  # noqa: F841
        raas["competencia_realizacao"]
    ) - pd.DateOffset(months=intervalo_inatividade - 1)
    return (
        raas
        # obter a data do primeiro procedimento realizado no estabelecimento
        .groupby_agg(
            by=["estabelecimento_nome", "usuario_id"],
            agg="min",
            agg_column_name="competencia_realizacao",
            new_column_name="competencia_primeiro_procedimento",
        )
        # eliminar RAAS anteriores à data de referência inicial
        .query("raas_competencia_inicio >= @DATA_INICIO")
        # para cada combinação CNS-estabelecimento-competência,
        # verificar se o usuário fez algum procedimento (esteve ativo no mês),
        # e se fez alguma atividade além de atendimento individual
        .groupby(
            [
                "estabelecimento_nome",
                "usuario_id",
                "competencia_realizacao",
            ]
        )
        .agg(
            competencia_primeiro_procedimento=(
                "competencia_primeiro_procedimento",
                "min",
            ),
            ativo_mes=("quantidade_registrada", any),
            fez_atividade=(
                "procedimento_nome",
                lambda procedimentos: (
                    # fez algum procedimento...
                    (len(procedimentos.index) > 0)
                    # ...e algum deles era diferente dos nomes de procedimentos
                    # de atendimento individual
                    and any(~procedimentos.isin(atendimento_individual))
                ),
            ),
        )
        .reset_index()
        .rename_column("competencia_realizacao", "competencia")
        # tornar explícitos os períodos (competências) em que o usuário esteve
        # inativo
        .complete(
            "competencia",
            (
                "estabelecimento_nome",
                "usuario_id",
                "competencia_primeiro_procedimento",
            ),
        )
        .fill_empty(["ativo_mes", "fez_atividade"], False)
        # definir a competência até a qual cada usuário será considerado um
        # recém-chegado - e, portanto, considerado para efeitos da taxa de abandono
        .transform_column(
            "competencia_primeiro_procedimento",
            function=lambda dt: (dt + pd.DateOffset(months=tempo_no_caps)),
            dest_column_name="data_deixou_de_ser_recem_chegado",
        )
        # definir um número de competências adicionais após a pessoa deixar de ser
        # recém chegada, que será utilizado para checar se o contato nos últimos
        # meses enquanto recém-chegado não foi seguido por abandono.
        # Ex.: se o prazo para ser considerado recém chegado é de 6 meses, e o
        # intervalo para ser considerado inativo é de 3 meses, um usuário pode ter
        # contato com o CAPS no 5º mês, e deixar de frequentar no 6º, 7º e 8º meses.
        # Nesse caso, deve ser considerado um abandono tendo como referência a
        # competência do sexto mês; e é necessário checar as competências até o 8º
        # mês (6 + 3 -1) para detectar esse abandono.
        .transform_column(
            "data_deixou_de_ser_recem_chegado",
            function=lambda dt: (
                dt + pd.DateOffset(months=intervalo_inatividade - 1)
            ),
            dest_column_name="data_max_calculo_inatividade",
        )
        # eliminar competências anteriores ao primeiro procedimento registrado em
        # RAAS
        .query("competencia >= competencia_primeiro_procedimento")
        # eliminar as competências posteriores à competência máxima necessária para
        # o cálculo da inatividade/abandono
        .query("competencia < data_max_calculo_inatividade")
        # eliminar os usuários que, durante os meses meses em que foram recém
        # chegados, não realizaram nenhuma atividade além de atendimentos
        # individuais.
        # Isso é necessário porque há usuários que ficam recebendo atendimento nos
        # CAPS apenas por algum tempo, enquanto esperam abrir vaga na fila para
        # referências ambulatoriais. Esse perfil não nos interessa para avaliar a
        # evasão.
        .update_where(
            # meses em que o usuario não é mais recem chegado, mas que estão no DF
            # para permitir os cálculos de inatividade; nesses casos, indicamos
            # a realização ou não de atividades como NaN, porque só importam as que
            # estiverem dentro do período em que o usuário é recente
            "competencia > data_deixou_de_ser_recem_chegado",
            target_column_name="fez_atividade",
            target_val=np.nan,
        )
        .groupby_agg(
            by=["estabelecimento_nome", "usuario_id"],
            agg=any,  # em algum mês fez atividade que não seja atend. individual?
            agg_column_name="fez_atividade",
            new_column_name="fez_atividade",
        )
        .query("fez_atividade == True")
        .remove_columns(["fez_atividade"])
        # ordenar os registros por estabelecimento, CNS e competência
        .sort_values(
            [
                "estabelecimento_nome",
                "usuario_id",
                "competencia",
            ]
        )
        .reset_index(drop=True)
        # determinar as competências que marcam o início de um período de inatividade
        .maxima_movel_retroativa(
            coluna_nome="ativo_mes",
            agrupar_por=["estabelecimento_nome", "usuario_id"],
            coluna_destino="ativo_periodo_seguinte",
            janela=intervalo_inatividade,
        )
        .groupby_agg(
            by=["estabelecimento_nome", "usuario_id"],
            agg=lambda s: s.shift(periods=1),
            agg_column_name="ativo_mes",
            new_column_name="ativo_ultimo_mes",
        )
        .fill_empty(["ativo_ultimo_mes"], False)
        .update_where(
            "ativo_ultimo_mes and (not ativo_periodo_seguinte)",
            target_column_name="inicia_inatividade",
            target_val=True,
        )
        .fill_empty(["inicia_inatividade"], False)
        # reproduzir a competência do início do primeiro periodo de inatividade em
        # todas as competências para o mesmo vínculo estabelecimento-usuário
        .join_apply(
            lambda i: (
                i["competencia"] if i["inicia_inatividade"] else np.nan
            ),
            new_column_name="_competencia_inicia_inatividade",
        )
        .groupby_agg(
            by=["estabelecimento_nome", "usuario_id"],
            agg="min",
            agg_column_name="_competencia_inicia_inatividade",
            new_column_name="competencia_inicia_inatividade",
        )
        .remove_columns(["_competencia_inicia_inatividade"])
        # remover o período adicional após o usuário deixar de ser recém-chegado
        .query("competencia < data_deixou_de_ser_recem_chegado")
        # determinar se o usuário chegou a abandonar enquanto recém-chegado
        .groupby_agg(
            by=["estabelecimento_nome", "usuario_id"],
            agg=any,
            agg_column_name="inicia_inatividade",
            new_column_name="abandonou",
        )
        # remover competências posteriores à perda de vínculo
        .query(
            "(not abandonou) | (competencia <= competencia_inicia_inatividade)"
        )
        # remover competências mais antigas, em que parte dos usuários
        # recém-chegados deram entrada antes da implementação da RAAS
        .query("competencia >= @data_minima")
        # remover competências mais recentes, em que ainda não houve tempo
        # suficiente para saber se o vínculo será perdido nos meses seguintes
        .query("competencia < @data_maxima")
        # # remover colunas intermediárias utilizadas para cálculo
        .remove_columns(
            [
                "ativo_mes",
                "ativo_ultimo_mes",
                "ativo_periodo_seguinte",
                "competencia_inicia_inatividade",
                "data_deixou_de_ser_recem_chegado",
                "data_max_calculo_inatividade",
            ]
        )
    )


def carregar_abandonos(sessao: Session, abandonos: pd.DataFrame) -> int:

    tabela_nome = "saude_mental._abandono_mensal"
    num_registros = len(abandonos)

    logger.info(
        "Preparando carregamento de {num_registros} registros de abandono "
        " para a tabela `{tabela_nome}`...",
        num_registros=num_registros,
        tabela_nome=tabela_nome,
    )
    conector = sessao.connection()
    abandonos.to_sql(
        name=tabela_nome.split(".")[-1],
        con=conector,
        schema=tabela_nome.split(".")[0],
        chunksize=1000,
        if_exists="replace",  # TODO!: mudar para append, removendo seletiva/e
        index=False,
        method="multi",
    )

    logger.info("OK.")
    return 0


def obter_abandonos(
    sessao: Session,
    unidade_geografica_id_sus: str,
    periodo_data_inicio: datetime,
    tempo_no_caps: int = 6,
    intervalo_inatividade: int = 3,
    remover_perfil_ambulatorial: bool = True,
    teste: bool = False,
) -> None:

    logger.info(
        "Iniciando caracterização de abandonos em CAPS no município de ID "
        + "{unidade_geografica_id_sus} na competencia de "
        + "{periodo_data_inicio:%m/%Y}...",
        unidade_geografica_id_sus=unidade_geografica_id_sus,
        periodo_data_inicio=periodo_data_inicio,
    )

    raas = consultar_raas(
        sessao=sessao,
        unidade_geografica_id_sus=unidade_geografica_id_sus,
    )

    usuarios_abandonaram = caracterizar_abandonos(
        raas=raas,
        tempo_no_caps=tempo_no_caps,
        intervalo_inatividade=intervalo_inatividade,
        remover_perfil_ambulatorial=remover_perfil_ambulatorial,
    )

    carregar_abandonos(
        sessao=sessao,
        abandonos=usuarios_abandonaram,
    )

    if not teste:
        sessao.commit()

    logger.info(
        "Processamento de dados sobre abandono finalizada para o município",
    )
