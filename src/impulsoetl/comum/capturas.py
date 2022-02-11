# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Metadados de capturas de dados públicos realizadas."""


from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from impulsoetl.bd import tabelas
from impulsoetl.comum.datas import agora_gmt_menos3, obter_proximo_periodo

capturas_agendadas = tabelas["configuracoes.capturas_agendadas"]


# TODO: registro do histórico de capturas
#
# capturas_realizadas = tabelas["logs.capturas_realizadas"]
#
# @lru_cache(5570)
# def obter_ultimas_capturas(
#     sessao: Session,
#     tabela_destino: str,
#     periodo_id: str,
#     apenas_bem_sucedidas: bool = True,
#     max_capturas: int | None = None,
# ) -> list[object]:
#     """Obtém as últimas capturas de um conjunto de dados para o BD da Impulso.

#     Argumentos:
#         sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
#             acessar a base de dados da ImpulsoGov.
#         tabela_destino: Nome da tabela de destino da operação de captura, no
#             padrão `nome_do_schema.nome_da_tabela`.
#         periodo_id: representação em hexadecimal do identificador único (UUID)
#             do período de referência dos dados capturados.
#         apenas_bem_sucedidas: indica se apenas as capturas marcadas como
#             bem-sucedidas devem ser retornadas. Habilitado por padrão.
#         max_capturas: número máximo de capturas mais recentes que devem ser
#             retornadas. Por padrão, o valor é `None`, o que corresponde a
#             retornar todas as capturas encontradas para os parâmetros
#             informados.

#     Retorna:
#         Uma lista de objetos que mapeiam os metadados das capturas de dados
#         públicos correspondentes aos parâmetros informados.

#     [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
#     """
#     condicoes = [
#         capturas_realizadas.c.tabela == tabela_destino,
#         capturas_realizadas.c.periodo_id == periodo_id,
#     ]
#     if apenas_bem_sucedidas:
#         condicoes.append(
#             capturas_realizadas.c.bem_sucedido == True,  # noqa: E712
#         )

#     capturas_relevantes = (
#         sessao.query(capturas_realizadas)
#         .filter(*condicoes)
#         .order_by(capturas_realizadas.c.a_partir_de.desc())
#     )

#     if max_capturas:
#         capturas_relevantes = capturas_relevantes.limit(max_capturas)

#     return capturas_relevantes.all()


def unidades_pendentes_por_periodo(
    sessao: Session,
    tabela_destino: str,
) -> dict[str, list[str]]:
    """Retorna os municípios pendentes para captura em cada período.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.

    Retorna:
        Um dicionário em que cada chave é o identificador de um período no
        banco de dados da ImpulsoGov, e cada valor é uma lista de
        identificadores das unidades geográficas que têm captura agendada para
        aquele período.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """
    agora = agora_gmt_menos3()
    agendamentos = (
        sessao.query(capturas_agendadas)
        .filter(capturas_agendadas.c.tabela == tabela_destino)
        .all()
    )
    unidades_por_periodo = defaultdict(list)
    for agendamento in agendamentos:
        if agendamento.a_partir_de <= agora:
            unidades_por_periodo[agendamento.periodo_id].append(
                agendamento.unidade_geografica_id,
            )
    return unidades_por_periodo


def atualizar_proxima_captura(
    sessao: Session,
    tabela_destino: str,
    unidade_geografica_id: str,
    atraso: int = 30,  # TODO: aceitar DateOffset, timedelta etc.
) -> int:
    """Atualiza o período e a data da próxima captura de um conjunto de dados.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        tabela_destino: Nome da tabela de destino da operação de captura, no
            padrão `nome_do_schema.nome_da_tabela`.
        unidade_geografica_id: representação hexadecimal do identificador único
            (UUID) da unidade geográfica de referência dos dados capturados.
        atraso: Número de dias após o término de cada período para que se
            iniciem as tentativas de captura dos dados daquela competência.
            Por padrão, o valor é `30` dias, o que significa que as tentativas
            de captura devem ser iniciadas aproximadamente um mês após a
            conclusão do período.

    Retorna:
        Um código de saída indicando o resultado da atualização. Se for
        bem-sucedida, o valor será `0`.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """

    linha_a_atualizar = (
        sessao.query(capturas_agendadas)
        .filter(
            capturas_agendadas.c.tabela == tabela_destino,
            capturas_agendadas.c.unidade_geografica_id
            == unidade_geografica_id,
        )
        .one()
    )

    proximo_periodo = obter_proximo_periodo(
        sessao=sessao,
        periodo_id=linha_a_atualizar.periodo_id,
    )

    nova_data = datetime(
        proximo_periodo.data_fim.year,
        proximo_periodo.data_fim.month,
        proximo_periodo.data_fim.day,
    )
    nova_data += timedelta(days=atraso)
    nova_data = nova_data.astimezone(tz=timezone(-timedelta(hours=3)))

    requisicao_atualizacao = (
        capturas_agendadas.update()
        .values(
            periodo_id=proximo_periodo.id,
            a_partir_de=nova_data,
        )
        .where(capturas_agendadas.c.id == linha_a_atualizar.id)
    )

    conector = sessao.connection()
    conector.execute(requisicao_atualizacao)

    return 0
