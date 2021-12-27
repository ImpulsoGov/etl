# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Define representações comuns aos vários modelos de objetos para ETL."""


from enum import Enum


class RacaCor(Enum):
    """Denominação de raça e cor, segundo a SIGTAP."""

    BRANCA = "01"
    PRETA = "02"
    PARDA = "03"
    AMARELA = "04"
    INDIGENA = "05"
    SEM_INFORMACAO = "90"


class Sexo(Enum):
    """"Classificação do sexo biológico, segundo a SIGTAP."""

    FEMININO = "F"
    IGNORADO = "I"
    MASCULINO = "M"
