# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Exceções relacionadas à interação com a interface do SISAB."""


class SisabExcecao(Exception):
    """Exceção base para os erros do SISAB."""

    pass


class SisabErroCompetenciaInexistente(SisabExcecao, ValueError):
    """A competência indicada não está disponível na interface do SISAB."""

    pass


class SisabErroPreenchimentoIncorreto(SisabExcecao, ValueError):
    """O formulário indicou violação de alguma regra de preenchimento."""

    pass


class SisabErroRotuloOuValorInexistente(SisabExcecao, ValueError):
    """Algum parâmetro indicado não está disponível na interface do SISAB."""

    pass
