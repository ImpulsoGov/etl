# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Testa os scripts de captura de dados para os produtos de saúde mental."""


from impulsoetl.scripts.geral import vinculos_disseminacao


def teste_vinculos_disseminacao(sessao):
    """Testa obter vínculos profissionais do CNES."""
    vinculos_disseminacao(sessao=sessao, teste=True)
