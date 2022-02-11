# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Define objetos utilizados em processos de ETL relativos à saúde mental.

Atributos:
    CID10_SAUDE_MENTAL: Códigos CID-10 relacionados a questões de saúde mental.

        Contém uma lista dos capítulos, grupos, categorias e subcategorias de
        condições de saúde relacionadas à saúde mental, de acordo com a
        [OMS (1993)][].

        Quando um nível mais genérico da estrutura CID-10 é utilizado (por
        exemplo, apenas o capítulo no lugar dos grupos e categorias), deve-se
        entender que todos as condições mais específicas contidas naquela
        estrutura são relacionadas à saúde mental.

[OMS (1993)]: https://apps.who.int/iris/handle/10665/37108
"""


from frozenlist import FrozenList

from impulsoetl.bd import tabelas

CID10_SAUDE_MENTAL: "FrozenList[str]" = FrozenList(
    [
        "F",  # Transtornos mentais (F00-F99)
        "X6",  # Auto-intoxicação intencional
        "X7",  # Lesão autoprovocada intencionalmente
        "X80",  # ... por precipitação de um lugar elevado
        "X81",  # ... por precipitação ou permanência diante de objeto em movimento
        "X82",  # ... por impacto de veículo a motor
        "X83",  # ... por outros meios especificados
        "X84",  # ... por meios não especificados
        "Z004",  # Exame psiquiátrico não especificado em outro local
        "Z032",  # Observação por suspeita de transtorno mental ou de comportamento
        "Z046",  # Exame psiquiátrico requisitado por autoridade
        "Z502",  # Reabilitação por uso de álcool
        "Z503",  # Reabilitação por uso de outras drogas
        "Z504",  # Psicoterapia não especificada em outro local
        "Z507",  # Terapia ocupacional e reabilitação profissional
        "Z508",  # Outras terapias de reabilitação para a vida diária
        "Z543",  # Convalescência após psicoterapia
        # TODO: continuar listagem.
    ],
)

# municipios_monitorados = tabelas["configuracoes.municipios_saude_mental"]
