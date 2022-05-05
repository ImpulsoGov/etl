
   
# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Declara representações das tabelas relativas ao SISAB."""
from impulsoetl.bd import tabelas

indicadores_equipe_validas = tabelas["dados_publicos.sisab_indicadores_municipios_equipes_validas"]
indicadores_equipe_homologadas = tabelas["dados_publicos.sisab_indicadores_municipios_equipes_homologadas"]
indicadores_todas_equipes = tabelas["dados_publicos.sisab_indicadores_municipios_equipe_todas"]
indicadores_regras = tabelas["previne_brasil.indicadores_regras"]