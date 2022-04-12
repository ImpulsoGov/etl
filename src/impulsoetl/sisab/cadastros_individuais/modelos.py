# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Declara representações das tabelas relativas ao SISAB."""

from impulsoetl.bd import tabelas

cadastros_equipe_validas = tabelas[
    "dados_publicos.sisab_cadastros_municipios_equipe_validas"
]
cadastros_equipe_homologadas = tabelas[
    "dados_publicos.sisab_cadastros_municipios_equipe_homologadas"
]
cadastros_todas_equipes = tabelas[
    "dados_publicos.sisab_cadastros_municipios_equipe_todas"
]
