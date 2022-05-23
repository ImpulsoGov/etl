# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Declara representações das tabelas relativas ao SISAB."""
from impulsoetl.bd import tabelas

parametros_municipios_equipe_validas = tabelas[
    "dados_publicos.sisab_cadastros_parametro_municipios_equipes_validas"
]
parametros_municipios_equipe_homologadas = tabelas[
    "dados_publicos.sisab_cadastros_parametro_municipios_equipes_homologadas"
]
parametros_equipes_equipe_validas = tabelas[
    "dados_publicos.sisab_cadastros_parametro_cnes_ine_equipes_validas"
]
parametros_equipes_equipe_homologadas = tabelas[
    "dados_publicos.sisab_cadastros_parametro_cnes_ine_equipes_equipe_homologadas"
]
