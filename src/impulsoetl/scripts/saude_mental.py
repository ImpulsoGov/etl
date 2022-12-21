#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Scripts para o produto de Saúde Mental."""


from prefect import flow
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.bd import tabelas
from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.siasus.bpa_i import obter_bpa_i
from impulsoetl.siasus.procedimentos import obter_pa
from impulsoetl.siasus.raas_ps import obter_raas_ps
from impulsoetl.sihsus.aih_rd import obter_aih_rd
from impulsoetl.sinan.violencia import obter_agravos_violencia
from impulsoetl.sisab.producao import obter_relatorio_producao

agendamentos = tabelas["configuracoes.capturas_agendamentos"]
capturas_historico = tabelas["configuracoes.capturas_historico"]


@flow(
    name="Rodar Agendamentos de Resolutividade da APS por Condição Avaliada",
    description=(
        "Lê as capturas agendadas para obter os desfechos dos atendimentos "
        + "individuais na Atenção Primária à Saúde, por problema/condição "
        + "avaliada, a partir do Sistema de Informação em Saúde da Atenção "
        + "Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def resolutividade_aps_por_condicao(
    sessao: Session,
    teste: bool = False,
) -> None:
    """Desfechos dos atendimentos individuais da APS por condição avaliada.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """
    habilitar_suporte_loguru()
    logger.info(
        "Capturando dados de resolutividade da APS (desfechos de atendimentos "
        + "individuais) por condição de saúde avaliada.",
    )

    operacao_id = "bdbeb1c4-bdc6-432f-a3b4-b6ca306e32c9"
    agendamentos_resolutividade_por_condicao = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id == operacao_id)
        .all()
    )

    for agendamento in agendamentos_resolutividade_por_condicao:
        obter_relatorio_producao(
            tabela_destino=agendamento.tabela_destino,
            variaveis=("Conduta", "Problema/Condição Avaliada"),
            unidades_geograficas_ids=[agendamento.unidade_geografica_id],
            unidade_geografica_tipo="Municípios",
            ano=agendamento.periodo_data_inicio.year,
            mes=agendamento.periodo_data_inicio.month,
            atualizar_captura=False,
            sessao=sessao,
            teste=teste,
        )
        if teste:  # evitar rodar muitas iterações
            break


@flow(
    name="Rodar Agendamentos de Produção por Tipo de Equipe da APS",
    description=(
        "Lê as capturas agendadas para obter a quantidade de contatos "
        + "assistenciais realizados na Atenção Primária à Saúde, por tipo da "
        + "produção realizada e por tipo de equipe, a partir do Sistema de "
        + "Informação em Saúde da Atenção Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def tipo_equipe_por_tipo_producao(
    sessao: Session,
    teste: bool = False,
) -> None:
    """Número de contatos assistenciais na APS por tipo de produção e equipe.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """
    habilitar_suporte_loguru()
    logger.info(
        "Capturando dados de atendimentos individuais) por condição de saúde avaliada.",
    )
    variaveis = ("Tipo de Equipe", "Tipo de Produção")
    operacao_id = "0f397c27-db38-4fd9-b097-3a9e25138b4c"
    agendamentos_producao_por_equipe = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id == operacao_id)
        .all()
    )
    for agendamento in agendamentos_producao_por_equipe:
        obter_relatorio_producao(
            sessao=sessao,
            tabela_destino=agendamento.tabela_destino,
            variaveis=variaveis,
            unidades_geograficas_ids=[agendamento.unidade_geografica_id],
            unidade_geografica_tipo=agendamento.unidade_geografica_tipo,
            ano=agendamento.periodo_data_inicio.year,
            mes=agendamento.periodo_data_inicio.month,
            tipo_producao=None,
            atualizar_captura=False,
            teste=teste,
        )
        if teste:
            break


@flow(
    name="Rodar Agendamentos de Arquivos de Disseminação da RAAS-PS",
    description=(
        "Lê as capturas agendadas para obter os arquivos de disseminação dos "
        + "Registros de Ações Ambulatoriais em Saúde - Psicossociais "
        + "(RAAS-PS), a partir do repositório público do Sistema de "
        + "Informações Ambulatoriais do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def raas_disseminacao(
    sessao: Session,
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando RAAS Psicossociais do SIASUS.",
    )
    operacao_ids = [
        "69bb7a34-05a8-4d9d-bc7e-c4e9e9722ece",
    ]
    agendamentos_raas = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id.in_(operacao_ids))
        .all()
    )
    for agendamento in agendamentos_raas:
        obter_raas_ps(
            sessao=sessao,
            uf_sigla=agendamento.uf_sigla,
            periodo_data_inicio=agendamento.periodo_data_inicio,
            tabela_destino=agendamento.tabela_destino,
            teste=teste,
            **agendamento.parametros,
        )
        if teste:
            break

        logger.info("Registrando captura bem-sucedida...")
        # NOTE: necessário registrar a operação de captura em nível de UF,
        # mesmo que o gatilho na tabela de destino no banco de dados já
        # registre a captura em nível dos municípios automaticamente quando há
        # a inserção de uma nova linha
        requisicao_inserir_historico = capturas_historico.insert(
            {
                "operacao_id": agendamento.operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()
        logger.info("OK.")


@flow(
    name="Rodar Agendamentos de Arquivos de Disseminação dos BPA-i's",
    description=(
        "Lê as capturas agendadas para obter os arquivos de disseminação dos "
        + "Boletins de Produção Ambulatorial - individualizados (BPA-i), a "
        + "partir do repositório público do Sistema de Informações "
        + "Ambulatoriais do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def bpa_i_disseminacao(
    sessao: Session,
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando BPAs individualizados do SIASUS.",
    )
    operacao_ids = [
        "50d46e1c-7fb3-4fbb-b495-825ff1f397d9",
        "063000e1-93e2-7c23-9bd0-1f0e7cf59178",
    ]
    agendamentos_bpa_i = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id.in_(operacao_ids))
        .all()
    )
    for agendamento in agendamentos_bpa_i:
        obter_bpa_i(
            sessao=sessao,
            uf_sigla=agendamento.uf_sigla,
            periodo_data_inicio=agendamento.periodo_data_inicio,
            tabela_destino=agendamento.tabela_destino,
            teste=teste,
            **agendamento.parametros,
        )
        if teste:
            break

        logger.info("Registrando captura bem-sucedida...")
        # NOTE: necessário registrar a operação de captura em nível de UF,
        # mesmo que o gatilho na tabela de destino no banco de dados já
        # registre a captura em nível dos municípios automaticamente quando há
        # a inserção de uma nova linha
        requisicao_inserir_historico = capturas_historico.insert(
            {
                "operacao_id": agendamento.operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()
        logger.info("OK.")


@flow(
    name=(
        "Rodar Agendamentos de Arquivos de Disseminação de Procedimentos "
        + "Ambulatoriais"
    ),
    description=(
        "Lê as capturas agendadas para obter os arquivos de disseminação dos "
        + "procedimentos ambulatoriais da atenção especializada, a partir do "
        + "repositório público do Sistema de Informações Ambulatoriais do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def procedimentos_disseminacao(
    sessao: Session,
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando procedimentos ambulatoriais do SIASUS.",
    )
    operacao_ids = [
        "f2a62b56-932a-431d-aee5-e3c0af33914f",
        "063000ce-23f5-7c29-a1cb-1d631ea26685",
    ]
    agendamentos_pa = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id.in_(operacao_ids))
        .all()
    )
    for agendamento in agendamentos_pa:
        obter_pa(
            sessao=sessao,
            uf_sigla=agendamento.uf_sigla,
            periodo_data_inicio=agendamento.periodo_data_inicio,
            tabela_destino=agendamento.tabela_destino,
            teste=teste,
            **agendamento.parametros,
        )
        if teste:
            break

        logger.info("Registrando captura bem-sucedida...")
        # NOTE: necessário registrar a operação de captura em nível de UF,
        # mesmo que o gatilho na tabela de destino no banco de dados já
        # registre a captura em nível dos municípios automaticamente quando há
        # a inserção de uma nova linha
        requisicao_inserir_historico = capturas_historico.insert(
            {
                "operacao_id": agendamento.operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()
        logger.info("OK.")


@flow(
    name="Rodar Agendamentos de Arquivos de Disseminação das AIH-RD's",
    description=(
        "Lê as capturas agendadas para obter os arquivos de disseminação das "
        + "Autorizações de Internação Hospitalar - reduzidas (AIH-RD), a "
        + "partir do repositório público do Sistema de Informações "
        + "Hospitalares do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def aih_reduzida_disseminacao(
    sessao: Session,
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando autorizações de internação hospitalar do SIHSUS.",
    )
    operacao_id = "0411c818-d189-4f2a-9aa2-7e2cac1b2b79"
    agendamentos_aih_rd = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id == operacao_id)
        .all()
    )
    for agendamento in agendamentos_aih_rd:
        obter_aih_rd(
            sessao=sessao,
            uf_sigla=agendamento.uf_sigla,
            periodo_data_inicio=agendamento.periodo_data_inicio,
            tabela_destino=agendamento.tabela_destino,
            teste=teste,
        )
        if teste:
            break

        logger.info("Registrando captura bem-sucedida...")
        # NOTE: necessário registrar a operação de captura em nível de UF,
        # mesmo que o gatilho na tabela de destino no banco de dados já
        # registre a captura em nível dos municípios automaticamente quando há
        # a inserção de uma nova linha
        requisicao_inserir_historico = capturas_historico.insert(
            {
                "operacao_id": operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()
        logger.info("OK.")


@flow(
    name=(
        "Rodar Agendamentos de Arquivos de Disseminação de Agravos de "
        + "Violência"
    ),
    description=(
        "Lê as capturas agendadas para obter os arquivos de disseminação das "
        + "notificações de agravo de violência, a partir do repositório "
        + "público do Sistema de Informação de Agravos de Notificação do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def agravos_violencia(
    sessao: Session,
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info("Capturando notificações de agravos de violência do SINAN.")

    operacao_ids = [
        "06324f18-aefd-770a-aa8b-9b4ca7681070",
    ]
    agendamentos_agravos_violencia = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id.in_(operacao_ids))
        .all()
    )
    for agendamento in agendamentos_agravos_violencia:
        obter_agravos_violencia(
            sessao=sessao,
            periodo_id=agendamento.periodo_id,
            periodo_data_inicio=agendamento.periodo_data_inicio,
            tabela_destino=agendamento.tabela_destino,
            teste=teste,
            **agendamento.parametros,
        )
        if teste:
            break

        logger.info("Registrando captura bem-sucedida...")
        # NOTE: necessário registrar a operação de captura em nível de país,
        # mesmo que o gatilho na tabela de destino no banco de dados já
        # registre a captura em nível dos municípios automaticamente quando há
        # a inserção de uma nova linha
        requisicao_inserir_historico = capturas_historico.insert(
            {
                "operacao_id": agendamento.operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()
        logger.info("OK.")
