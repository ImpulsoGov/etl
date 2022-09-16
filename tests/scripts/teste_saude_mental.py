# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Testa os scripts de captura de dados para os produtos de saúde mental."""


from impulsoetl.scripts.saude_mental import (
    agravos_violencia,
    aih_reduzida_disseminacao,
    bpa_i_disseminacao,
    procedimentos_disseminacao,
    raas_disseminacao,
    resolutividade_aps_por_condicao,
)


def teste_resolutividade_aps_por_condicao(sessao):
    """Testa obter desfechos dos atendimentos da APS por condição avaliada."""
    resolutividade_aps_por_condicao(sessao=sessao, teste=True)


def teste_raas_disseminacao(sessao):
    """Testa obter RAAS Psicossociais."""
    raas_disseminacao(sessao=sessao, teste=True)


def teste_bpa_i_disseminacao(sessao):
    """Testa obter Boletins de Produção Ambulatorial individualizados."""
    bpa_i_disseminacao(sessao=sessao, teste=True)


def teste_procedimentos_disseminacao(sessao):
    """Testa obter procedimentos ambulatoriais."""
    procedimentos_disseminacao(sessao=sessao, teste=True)


def teste_agravos_violencia(sessao):
    """Testa obter notificações de agravos de violência."""
    agravos_violencia(sessao=sessao, teste=True)


def teste_aih_reduzida_disseminacao(sessao):
    """Testa obter autorizações de internação hospitalar."""
    aih_reduzida_disseminacao(sessao=sessao, teste=True)
