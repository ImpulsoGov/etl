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


def teste_resolutividade_aps_por_condicao():
    """Testa obter desfechos dos atendimentos da APS por condição avaliada."""
    resolutividade_aps_por_condicao(teste=True)


def teste_raas_disseminacao():
    """Testa obter RAAS Psicossociais."""
    raas_disseminacao(teste=True)


def teste_bpa_i_disseminacao():
    """Testa obter Boletins de Produção Ambulatorial individualizados."""
    bpa_i_disseminacao(teste=True)


def teste_procedimentos_disseminacao():
    """Testa obter procedimentos ambulatoriais."""
    procedimentos_disseminacao(teste=True)


def teste_agravos_violencia():
    """Testa obter notificações de agravos de violência."""
    agravos_violencia(teste=True)


def teste_aih_reduzida_disseminacao():
    """Testa obter autorizações de internação hospitalar."""
    aih_reduzida_disseminacao(teste=True)
