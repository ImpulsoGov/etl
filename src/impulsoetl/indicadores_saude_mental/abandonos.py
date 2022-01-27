# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém indicadores sobre o abandono de usuários recentes em CAPS."""


from __future__ import annotations

import json

# import uuid
from datetime import datetime

import janitor  # noqa: F401  # nopycln: import
import numpy as np
import pandas as pd
import pandas_flavor as pf
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from impulsoetl.comum.datas import periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.indicadores_saude_mental.comum import consultar_painel_raas
from impulsoetl.indicadores_saude_mental.modelos import (
    abandonos as tabela_destino,
)
from impulsoetl.loggers import logger

colunas_a_agrupar = [
    "competencia_realizacao",
    "servico_descricao",
    "servico_classificacao_descricao",
    "estabelecimento_tipo_descricao",
    "estabelecimento_nome",
    "encaminhamento_origem_descricao",
    "usuario_sexo",
    "usuario_raca_cor",
    "usuario_faixa_etaria",
    "usuario_situacao_rua",
    "usuario_substancias_abusa",
    "cid_descricao",
    "cid_grupo_descricao_curta",
]


@pf.register_dataframe_method
def _maxima_movel_retroativa(
    df: pd.DataFrame,
    coluna_nome: str,
    agrupar_por: list,
    coluna_destino: str,
    janela: int,
) -> pd.DataFrame:
    """Retorna, em cada linha, o valor máximo da próxima janela de registros.

    Argumentos:
        coluna_nome: Nome da coluna de cujos valores se quer obter a máxima
            móvel.
        agrupar_por: Lista de colunas para se agrupar o DataFrame. Cada grupo
            conta como uma unidade na janela usada para calcular a média móvel.
        coluna_destino: Nome da coluna onde será adicionada a média móvel
            retroativa no DataFrame de origem.
        janela: Número de grupos contados na janela para cálculo da máxima
            móvel.
    """
    df_copia = df.copy()
    df_copia[coluna_destino] = df_copia.groupby(agrupar_por)[
        coluna_nome
    ].transform(
        lambda s: (
            s.rolling(janela, min_periods=1).max()
            # desloca os períodos retroativamente
            # ver: https://stackoverflow.com/a/38056328/7733563
            .shift(periods=1 - janela)
        ),
    )
    return df_copia


def caracterizar_abandonos(
    sessao: Session,
    painel_raas: pd.DataFrame,  # TODO: substituir por dados brutos de RAAS
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
    data_inicio_raas = pd.Timestamp(2013, 1, 1).tz_localize(
        "America/Sao_Paulo"
    )
    data_minima = (  # noqa: F841 usado c/ .query()
        data_inicio_raas + pd.DateOffset(months=tempo_no_caps)
    )

    # as competências mais recentes não devem ser utilizadas porque ainda não houve
    # tempo de determinar se o usuário entrou em inatividade
    data_maxima = max(  # noqa: F841 usado c/ .query()
        painel_raas["competencia_realizacao"]
    ) - pd.DateOffset(months=intervalo_inatividade - 1)
    return (
        painel_raas
        # renomear colunas para o futuro padrão do banco de dados
        .rename_column("municipio_id", "municipio_id_sus")
        .rename_column("competencia_realizacao", "periodo_data_inicio")
        .rename_column("usuario_id", "usuario_cns_criptografado")
        # obter a data do primeiro procedimento realizado no estabelecimento
        .groupby_agg(
            by=[
                "municipio_id_sus",
                "estabelecimento_nome",
                "usuario_cns_criptografado",
            ],
            agg="min",
            agg_column_name="periodo_data_inicio",
            new_column_name="primeiro_procedimento_periodo_data_inicio",
        )
        # eliminar RAAS anteriores à data de referência inicial
        .query("raas_competencia_inicio >= @data_inicio_raas")
        # para cada combinação CNS-estabelecimento-competência,
        # verificar se o usuário fez algum procedimento (esteve ativo no mês),
        # e se fez alguma atividade além de atendimento individual
        .groupby(
            [
                "municipio_id_sus",
                "estabelecimento_nome",
                "usuario_cns_criptografado",
                "periodo_data_inicio",
            ]
        )
        .agg(
            primeiro_procedimento_periodo_data_inicio=(
                "primeiro_procedimento_periodo_data_inicio",
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
        # tornar explícitos os períodos (competências) em que o usuário esteve
        # inativo
        .complete(
            "periodo_data_inicio",
            (
                "municipio_id_sus",
                "estabelecimento_nome",
                "usuario_cns_criptografado",
                "primeiro_procedimento_periodo_data_inicio",
            ),
        )
        .fill_empty(["ativo_mes", "fez_atividade"], False)
        # definir a competência até a qual cada usuário será considerado um
        # recém-chegado - e, portanto, considerado para efeitos da taxa de abandono
        .transform_column(
            "primeiro_procedimento_periodo_data_inicio",
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
        .query(
            "periodo_data_inicio >= primeiro_procedimento_periodo_data_inicio"
        )
        # eliminar as competências posteriores à competência máxima necessária para
        # o cálculo da inatividade/abandono
        .query("periodo_data_inicio < data_max_calculo_inatividade")
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
            "periodo_data_inicio > data_deixou_de_ser_recem_chegado",
            target_column_name="fez_atividade",
            target_val=np.nan,
        )
        .groupby_agg(
            by=["estabelecimento_nome", "usuario_cns_criptografado"],
            agg=any,  # em algum mês fez atividade que não seja atend. individual?
            agg_column_name="fez_atividade",
            new_column_name="fez_atividade",
        )
        .query("fez_atividade == True")
        .remove_columns(["fez_atividade"])
        # ordenar os registros por estabelecimento, CNS e competência
        .sort_values(
            [
                "municipio_id_sus",
                "estabelecimento_nome",
                "usuario_cns_criptografado",
                "periodo_data_inicio",
            ]
        )
        .reset_index(drop=True)
        # determinar as competências que marcam o início de um período de inatividade
        ._maxima_movel_retroativa(
            coluna_nome="ativo_mes",
            agrupar_por=[
                "municipio_id_sus",
                "estabelecimento_nome",
                "usuario_cns_criptografado",
            ],
            coluna_destino="ativo_periodo_seguinte",
            janela=intervalo_inatividade,
        )
        .astype({"ativo_periodo_seguinte": "bool"})
        .groupby_agg(
            by=[
                "municipio_id_sus",
                "estabelecimento_nome",
                "usuario_cns_criptografado",
            ],
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
                i["periodo_data_inicio"] if i["inicia_inatividade"] else np.nan
            ),
            new_column_name="_competencia_inicia_inatividade",
        )
        .groupby_agg(
            by=[
                "municipio_id_sus",
                "estabelecimento_nome",
                "usuario_cns_criptografado",
            ],
            agg="min",
            agg_column_name="_competencia_inicia_inatividade",
            new_column_name="competencia_inicia_inatividade",
        )
        .remove_columns(["_competencia_inicia_inatividade"])
        # remover o período adicional após o usuário deixar de ser recém-chegado
        .query("periodo_data_inicio < data_deixou_de_ser_recem_chegado")
        # determinar se o usuário chegou a abandonar enquanto recém-chegado
        .groupby_agg(
            by=[
                "municipio_id_sus",
                "estabelecimento_nome",
                "usuario_cns_criptografado",
            ],
            agg=any,
            agg_column_name="inicia_inatividade",
            new_column_name="abandonou",
        )
        # remover competências posteriores à perda de vínculo
        .query(
            "(not abandonou) "
            + "| (periodo_data_inicio <= competencia_inicia_inatividade)"
        )
        # remover competências mais antigas, em que parte dos usuários
        # recém-chegados deram entrada antes da implementação da RAAS; ao mesmo
        # tempo, remover competências mais recentes, em que ainda não houve
        # tempo suficiente para saber se o vínculo será perdido nos meses
        # seguintes
        .query("@data_minima <= periodo_data_inicio < @data_maxima")
        # adicionar identificadores do período e da unidade geografica
        .transform_column(
            "periodo_data_inicio",
            dest_column_name="periodo_id",
            function=lambda dt: periodo_por_data(sessao=sessao, data=dt).id,
        )
        .transform_column(
            "municipio_id_sus",
            dest_column_name="unidade_geografica_id",
            function=lambda id_sus: id_sus_para_id_impulso(
                sessao=sessao,
                id_sus=id_sus,
            ),
        )
        # # remover colunas intermediárias utilizadas para cálculo
        .remove_columns(
            [
                "ativo_mes",
                "ativo_ultimo_mes",
                "ativo_periodo_seguinte",
                "competencia_inicia_inatividade",
                "data_deixou_de_ser_recem_chegado",
                "data_max_calculo_inatividade",
                "periodo_data_inicio",
                "primeiro_procedimento_periodo_data_inicio",
                "municipio_id_sus",
            ],
        )
        # # adicionar id
        # .add_column("id", str())
        # .transform_column("id", function=lambda _: uuid.uuid4().hex)
    )


def carregar_abandonos(
    sessao: Session,
    abandonos: pd.DataFrame,
    passo: int = 1000,
) -> int:
    """Carrega um arquivo de disseminação de procedimentos ambulatoriais no BD.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        abandonos: [`DataFrame`][] contendo os dados de abandonos a serem
            carregados na tabela de destino, já no formato utilizado pelo banco
            de dados da ImpulsoGov (conforme retornado pela função
            [`caracterizar_abandonos()`][]).
        passo: Indica quantos registros devem ser enviados para a base de dados
            de cada vez.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`transformar_pa()`]: impulsoetl.indicadores_saude_mental.abandonos.caracterizar_abandonos
    """

    tabela_nome = tabela_destino.key
    num_registros = len(abandonos)
    logger.info(
        "Preparando carregamento de {num_registros} registros de abandono "
        " para a tabela `{tabela_nome}`...",
        num_registros=num_registros,
        tabela_nome=tabela_nome,
    )

    logger.info("Processando dados para JSON e de volta para um dicionário...")
    registros = json.loads(
        abandonos.to_json(
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
        requisicao_insercao = insert(
            tabela_destino, bind=sessao.get_bind()
        ).values(subconjunto_registros)

        # colunas que devem ser atualizadas se já existir algum registro com a
        # mesma combinação de CNS, município, estabelecimento e competência
        restricao_unicos = tabela_nome.split(".", maxsplit=1)[1] + "_un"
        colunas_a_atualizar = [
            # "primeiro_procedimento_periodo_data_inicio",
            "inicia_inatividade",
            "abandonou",
        ]

        # definir UPSERT de registros
        requisicao_inserir_ou_atualizar = (
            requisicao_insercao.on_conflict_do_update(
                constraint=restricao_unicos,
                set_={
                    col.name: col
                    for col in requisicao_insercao.excluded
                    if col.name in colunas_a_atualizar
                },
            )
        )
        try:
            conector.execute(requisicao_inserir_ou_atualizar)
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
        + "{unidade_geografica_id_sus} até a competência de "
        + "{periodo_data_inicio:%m/%Y}...",
        unidade_geografica_id_sus=unidade_geografica_id_sus,
        periodo_data_inicio=periodo_data_inicio,
    )

    painel_raas = consultar_painel_raas(
        sessao=sessao,
        unidade_geografica_id_sus=unidade_geografica_id_sus,
    )

    usuarios_abandonaram = caracterizar_abandonos(
        sessao=sessao,
        painel_raas=painel_raas,
        tempo_no_caps=tempo_no_caps,
        intervalo_inatividade=intervalo_inatividade,
        remover_perfil_ambulatorial=remover_perfil_ambulatorial,
    )

    if teste:
        passo = 10
        usuarios_abandonaram = usuarios_abandonaram.iloc[
            : min(1000, len(usuarios_abandonaram)),
        ]
        if len(usuarios_abandonaram) == 1000:
            logger.warning(
                "Arquivo de procedimentos ambulatoriais truncado para 1000 "
                + "registros para fins de teste."
            )
    else:
        passo = 1000

    carregar_abandonos(
        sessao=sessao,
        abandonos=usuarios_abandonaram,
        passo=passo,
    )

    if not teste:
        sessao.commit()

    logger.info(
        "Processamento de dados sobre abandono finalizada para o município",
    )
