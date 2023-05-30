#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Scripts para o produto Impulso Previne."""


from prefect import flow

from impulsoetl import __VERSION__
from impulsoetl.bd import Sessao, tabelas
from impulsoetl.egestor.relatorio_financiamento.principal import (
    obter_relatorio_financiamento,
)
from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.sisab.cadastros_individuais import obter_cadastros_individuais
from impulsoetl.sisab.indicadores_municipios.principal import (
    obter_indicadores_desempenho,
)
from impulsoetl.sisab.parametros_cadastro.principal import obter_parametros
from impulsoetl.sisab.relatorio_saude_producao.principal import (
    obter_relatorio_producao_por_profissionais_reduzidos,
)
from impulsoetl.sisab.relatorio_saude_producao.principal_outros import (
    obter_relatorio_producao_por_profissionais_outros,
)
from impulsoetl.sisab.relatorio_validacao_producao.principal import (
    obter_validacao_producao,
)

agendamentos = tabelas["configuracoes.capturas_agendamentos"]
capturas_historico = tabelas["configuracoes.capturas_historico"]


@flow(
    name="Rodar Agendamentos de Cadastros das Equipes Válidas",
    description=(
        "Lê as capturas agendadas para obter os cadastros de equipes válidas "
        + "do Sistema de Informação em Saúde da Atenção Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def cadastros_municipios_equipe_validas(
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando Cadastros de equipes válidas por município.",
    )
    # este já é o ID definitivo da operação!
    operacao_id = "da6bf13a-2acd-44c1-a3e2-21ab071fc8a3"
    visao_equipe = "equipes-validas"
    with Sessao() as sessao:
        agendamentos_cadastros = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )

        for agendamento in agendamentos_cadastros:
            obter_cadastros_individuais(
                sessao=sessao,
                visao_equipe=visao_equipe,
                periodo_data=agendamento.periodo_data_inicio,
                periodo_id=agendamento.periodo_id,
                periodo_codigo=agendamento.periodo_codigo,
                tabela_destino=agendamento.tabela_destino,
                operacao_id=operacao_id,
            )
            if teste:
                sessao.rollback()
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
    name="Rodar Agendamentos de Cadastros das Equipes Homologadas",
    description=(
        "Lê as capturas agendadas para obter os cadastros de equipes "
        + "homologadas do Sistema de Informação em Saúde da Atenção Básica do "
        + "SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def cadastros_municipios_equipe_homologada(
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando Cadastros de equipes válidas por município.",
    )

    operacao_id = "c668a75e-9eeb-4176-874b-98d7553222f2"
    visao_equipe = "equipes-homologadas"
    with Sessao() as sessao:
        agendamentos_cadastros = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )

        for agendamento in agendamentos_cadastros:
            obter_cadastros_individuais(
                sessao=sessao,
                visao_equipe=visao_equipe,
                periodo_data=agendamento.periodo_data_inicio,
                periodo_id=agendamento.periodo_id,
                periodo_codigo=agendamento.periodo_codigo,
                tabela_destino=agendamento.tabela_destino,
                operacao_id=operacao_id,
            )
            if teste:
                sessao.rollback()
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
    name="Rodar Agendamentos de Cadastros das Equipes de APS",
    description=(
        "Lê as capturas agendadas para obter os cadastros de todas as equipes "
        + "de Atenção Primária à Saúde do Sistema de Informação em Saúde da "
        + "Atenção Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def cadastros_municipios_equipe_todas(
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando Cadastros de equipes válidas por município.",
    )

    operacao_id = "180ae562-2e34-4ae7-bff4-31ded6f0b418"
    visao_equipe = "todas-equipes"
    with Sessao() as sessao:
        agendamentos_cadastros = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )

        for agendamento in agendamentos_cadastros:
            obter_cadastros_individuais(
                sessao=sessao,
                visao_equipe=visao_equipe,
                periodo_data=agendamento.periodo_data_inicio,
                periodo_id=agendamento.periodo_id,
                periodo_codigo=agendamento.periodo_codigo,
                tabela_destino=agendamento.tabela_destino,
                operacao_id=operacao_id,
            )
            if teste:
                sessao.rollback()
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
        "Rodar Agendamentos de Parâmetros de Cadastros das Equipes Válidas "
        + "(por município)"
    ),
    description=(
        "Lê as capturas agendadas para obter os parâmetros de cadastros das "
        + "equipes válidas do Sistema de Informação em Saúde da Atenção "
        + "Básica do SUS, com granularidade por município."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def parametros_municipios_equipes_validas(
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando parâmetros de cadastros por município.",
    )

    operacao_id = "c07a7a29-cacf-4102-9a28-b674ae0ec609"
    visao_equipe = "equipes-validas"
    nivel_agregacao = "municipios"

    with Sessao() as sessao:
        agendamentos_cadastros = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )

        for agendamento in agendamentos_cadastros:
            periodo = agendamento.periodo_data_inicio
            obter_parametros(
                sessao=sessao,
                visao_equipe=visao_equipe,
                periodo=periodo,
                nivel_agregacao=nivel_agregacao,
                teste=teste,
            )
            if teste:
                sessao.rollback()
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
        "Rodar Agendamentos de Parâmetros de Cadastros das Equipes "
        + "Homologadas (por município)"
    ),
    description=(
        "Lê as capturas agendadas para obter os parâmetros de cadastros das "
        + "equipes homologadas do Sistema de Informação em Saúde da Atenção "
        + "Básica do SUS, com granularidade por município."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def parametros_municipios_equipes_homologada(
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando parâmetros de cadastros por município.",
    )

    operacao_id = "8f593199-fcef-4023-b79a-0ed7f9050cd2"
    visao_equipe = "equipes-homologadas"
    nivel_agregacao = "municipios"

    with Sessao() as sessao:
        agendamentos_cadastros = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )

        for agendamento in agendamentos_cadastros:
            periodo = agendamento.periodo_data_inicio
            obter_parametros(
                sessao=sessao,
                visao_equipe=visao_equipe,
                periodo=periodo,
                nivel_agregacao=nivel_agregacao,
                teste=teste,
            )
            if teste:
                sessao.rollback()
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
        "Rodar Agendamentos de Parâmetros de Cadastros das Equipes "
        + "Homologadas (por equipe)"
    ),
    description=(
        "Lê as capturas agendadas para obter os parâmetros de cadastros das "
        + "equipes homologadas do Sistema de Informação em Saúde da Atenção "
        + "Básica do SUS, com granularidade por equipe."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def parametros_cne_ine_equipes_homologada(
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando parâmetros de cadastros por estabelecimento e equipe.",
    )

    operacao_id = "dcb03493-8ad2-4f48-bd3b-4022fc33c2c2"
    visao_equipe = "equipes-homologadas"
    nivel_agregacao = "estabelecimentos_equipes"

    with Sessao() as sessao:
        agendamentos_cadastros = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )

        for agendamento in agendamentos_cadastros:
            periodo = agendamento.periodo_data_inicio
            obter_parametros(
                sessao=sessao,
                visao_equipe=visao_equipe,
                periodo=periodo,
                nivel_agregacao=nivel_agregacao,
                teste=teste,
            )
            if teste:
                sessao.rollback()
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
        "Rodar Agendamentos de Parâmetros de Cadastros das Equipes Válidas "
        + "(por equipe)"
    ),
    description=(
        "Lê as capturas agendadas para obter os parâmetros de cadastros das "
        + "equipes válidas do Sistema de Informação em Saúde da Atenção "
        + "Básica do SUS, com granularidade por equipe."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def parametros_cnes_ine_equipes_validas(
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando parâmetros de cadastros por estabelecimento e equipe.",
    )

    operacao_id = "3a61f9ca-c32f-4844-b6ac-a115bd8e4b5a"
    visao_equipe = "equipes-validas"
    nivel_agregacao = "estabelecimentos_equipes"

    with Sessao() as sessao:
        agendamentos_cadastros = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )

        for agendamento in agendamentos_cadastros:
            periodo = agendamento.periodo_data_inicio
            obter_parametros(
                sessao=sessao,
                visao_equipe=visao_equipe,
                periodo=periodo,
                nivel_agregacao=nivel_agregacao,
                teste=teste,
            )
            if teste:
                sessao.rollback()
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
    name=("Rodar Agendamentos de Indicadores do Previne das Equipes Válidas"),
    description=(
        "Lê as capturas agendadas para obter os indicadores do Previne Brasil "
        + "das equipes válidas do Sistema de Informação em Saúde da Atenção "
        + "Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def indicadores_municipios_equipe_validas(
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando Indicadores municipais conisderando apenas equipes "
        + "válidas.",
    )
    # este já é o ID definitivo da operação!
    operacao_id = "133e8b75-f801-42f5-88de-611c3a1d0aa7"
    visao_equipe = "equipes-validas"

    with Sessao() as sessao:
        agendamentos_cadastros = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )

        for agendamento in agendamentos_cadastros:
            obter_indicadores_desempenho(
                sessao=sessao,
                visao_equipe=visao_equipe,
                periodo_data=agendamento.periodo_data_fim,
                periodo_id=agendamento.periodo_id,
                periodo_codigo=agendamento.periodo_codigo,
                operacao_id=operacao_id,
                tabela_destino=agendamento.tabela_destino,
            )
            if teste:
                sessao.rollback()
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
        "Rodar Agendamentos de Indicadores do Previne das Equipes Homologadas"
    ),
    description=(
        "Lê as capturas agendadas para obter os indicadores do Previne Brasil "
        + "das equipes homologadas do Sistema de Informação em Saúde da "
        + "Atenção Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def indicadores_municipios_equipes_homologadas(
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando Cadastros de equipes válidas por município.",
    )

    operacao_id = "584b190b-7a4c-4577-b617-1d847655affc"
    visao_equipe = "equipes-homologadas"

    with Sessao() as sessao:
        agendamentos_cadastros = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )

        for agendamento in agendamentos_cadastros:
            obter_indicadores_desempenho(
                sessao=sessao,
                visao_equipe=visao_equipe,
                periodo_data=agendamento.periodo_data_fim,
                periodo_id=agendamento.periodo_id,
                periodo_codigo=agendamento.periodo_codigo,
                operacao_id=operacao_id,
                tabela_destino=agendamento.tabela_destino,
            )
            if teste:
                sessao.rollback()
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
    name=("Rodar Agendamentos de Indicadores do Previne das Equipes de APS"),
    description=(
        "Lê as capturas agendadas para obter os indicadores do Previne Brasil "
        + "de todas as equipes de Atenção Primária à Saúde do Sistema de "
        + "Informação em Saúde da Atenção Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def indicadores_municipios_equipe_todas(
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando Cadastros de equipes válidas por município.",
    )

    operacao_id = "9d6b0b5d-bae7-4785-8c7b-ff55dc4386e0"
    visao_equipe = "todas-equipes"

    with Sessao() as sessao:
        agendamentos_cadastros = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )

        for agendamento in agendamentos_cadastros:
            obter_indicadores_desempenho(
                sessao=sessao,
                visao_equipe=visao_equipe,
                periodo_data=agendamento.periodo_data_fim,
                periodo_id=agendamento.periodo_id,
                periodo_codigo=agendamento.periodo_codigo,
                operacao_id=operacao_id,
                tabela_destino=agendamento.tabela_destino,
            )
            if teste:
                sessao.rollback()
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
    name=("Rodar Agendamentos de Relatórios de Validação"),
    description=(
        "Lê as capturas agendadas para obter os relatórios de validação da "
        + "produção da Atenção Primária à Saúde a partir do Sistema de "
        + "Informação em Saúde da Atenção Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def validacao_producao(
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    # este já é o ID definitivo da operação!
    operacao_id = "c577c9fd-6a8e-43e3-9d65-042ad2268cf0"

    # Ler agendamentos e rodar ETL para cada agendamento pendente
    # ...
    agendamentos = tabelas["configuracoes.capturas_agendamentos"]
    with Sessao() as sessao:
        agendamentos_relatorio_validacao = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )
        sessao.commit()

        logger.info("Leitura dos Agendamentos ok!")

        for agendamento in agendamentos_relatorio_validacao:
            obter_validacao_producao(
                sessao=sessao,
                periodo_competencia=agendamento.periodo_data_inicio,
                periodo_id=agendamento.periodo_id,
                periodo_codigo=agendamento.periodo_codigo,
                tabela_destino=agendamento.tabela_destino,
                operacao_id=operacao_id,
            )

            if teste:  # evitar rodar muitas iterações
                sessao.rollback()
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
    name=("Rodar Agendamentos de Relatórios de Financiamento"),
    description=(
        "Lê as capturas agendadas para obter os relatórios de financiamento "
        + "do eGestor Atenção Básica."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def egestor_financiamento(
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    operacoes_id = [
        "0635c378-835f-70a2-a82a-0cb13ade9559",
        "0635c378-85f3-7711-8c9c-7e6ee68ed0ed",
        "0635c378-8856-7e8b-819c-9accfc29b0d1",
        "0635c385-56ca-7fc4-b39c-81c84fcd790e",
        "0635c385-5943-7a54-880d-193384d2447c",
        "0635c385-5bb1-7bce-8eb4-83ad9d234726",
        "0635c38b-df79-7b13-865e-db5334aab8c6",
        "0635c342-d850-7ecb-b083-6170d49abd02",
        "0635c342-dffb-7e98-8c37-0e2896127d80",
        "063519ff-066c-7cc4-8e5a-2089ebc51d23",
        "0635c366-300c-7d96-9802-344915cddb63",
        "0638d2de-1264-7cf9-a6e1-e414b1f89271",
    ]

    # Ler agendamentos e rodar ETL para cada agendamento pendente
    # ...
    with Sessao() as sessao:
        for operacao_id in operacoes_id:
            agendamentos = tabelas["configuracoes.capturas_agendamentos"]
            agendamentos_relatorio_egestor = (
                sessao.query(agendamentos)
                .filter(agendamentos.c.operacao_id == operacao_id)
                .all()
            )
            sessao.commit()

            logger.info("Leitura dos Agendamentos ok!")
            for agendamento in agendamentos_relatorio_egestor:
                obter_relatorio_financiamento(
                    sessao=sessao,
                    periodo_id=agendamento.periodo_id,
                    tabela_destino=agendamento.tabela_destino,
                    periodo_mes=agendamento.periodo_data_inicio,
                    operacao_id=operacao_id,
                )

                if teste:  # evitar rodar muitas iterações
                    sessao.rollback()
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
        "Rodar Agendamentos do Relatório de Produção do SISAB - Profissionais Reduzidos"
    ),
    description=(
        "Lê as capturas agendadas para obter o Relatório de Produção de Saúde do SISAB "
        + "para todos os municípios, filtrados por Tipo de Equipe, Categoria Profissional,"
        + "Condição Avaliada, Tipo de Atendimento e Conduta"
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def relatorio_producao_saude_profissionais_reduzidos(
    teste: bool = True,
) -> None:
    habilitar_suporte_loguru()
    operacao_id = "063e2878-3247-78a7-83dd-1d291156cdf6"

    agendamentos = tabelas["configuracoes.capturas_agendamentos"]
    with Sessao() as sessao:
        agendamentos_relatorio_producao_saude = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )
        sessao.commit()

        logger.info("Leitura dos Agendamentos ok!")
        for agendamento in agendamentos_relatorio_producao_saude:
            obter_relatorio_producao_por_profissionais_reduzidos(
                sessao=sessao,
                tabela_destino=agendamento.tabela_destino,
                periodo_competencia=agendamento.periodo_data_inicio,
                periodo_id=agendamento.periodo_id,
                unidade_geografica_id=agendamento.unidade_geografica_id,
            )

            if teste:
                sessao.rollback()
                break

            logger.info("Registrando captura bem-sucedida...")
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
        "Rodar Agendamentos do Relatório de Produção do SISAB - Profissionais Outros"
    ),
    description=(
        "Lê as capturas agendadas para obter o Relatório de Produção de Saúde do SISAB "
        + "para todos os municípios, filtrados por Tipo de Equipe, Categoria Profissional,"
        + "Condição Avaliada, Tipo de Atendimento e Conduta"
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def relatorio_producao_saude_profissionais_outros(
    teste: bool = True,
) -> None:
    habilitar_suporte_loguru()
    operacao_id = "06423293-7fac-7493-b209-e5aa489879fb"

    agendamentos = tabelas["configuracoes.capturas_agendamentos"]
    with Sessao() as sessao:
        agendamentos_relatorio_producao_saude = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )
        sessao.commit()

        logger.info("Leitura dos Agendamentos ok!")
        for agendamento in agendamentos_relatorio_producao_saude:
            obter_relatorio_producao_por_profissionais_outros(
                sessao=sessao,
                tabela_destino=agendamento.tabela_destino,
                periodo_competencia=agendamento.periodo_data_inicio,
                periodo_id=agendamento.periodo_id,
                unidade_geografica_id=agendamento.unidade_geografica_id,
            )

            if teste:
                sessao.rollback()
                break

            logger.info("Registrando captura bem-sucedida...")
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
            
