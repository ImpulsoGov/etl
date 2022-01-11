# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Utilidades para lidar com dados textuais."""


from __future__ import annotations

import re

from toolz.functoolz import compose_left
from unidecode import unidecode


def normalizar_texto(texto: str, separador: str = "_", caixa="baixa") -> str:
    """Normaliza o texto para caixa baixa e removendo caracteres especiais."""
    texto_limpo = re.sub("[^a-zA-Z0-9]", separador, unidecode(texto).strip())
    # garantir que não haja dois separadores seguidos
    if len(separador) > 0:
        texto_limpo = re.sub(separador + "{2,}", separador, texto_limpo)
    if caixa == "alta":
        return texto_limpo.upper()
    elif caixa == "baixa":
        return texto_limpo.lower()
    else:
        raise ValueError(
            "O valor do argumento `caixa` deve ser 'alta' ou 'baixa'.",
        )


def remover_palavras_vazias(texto: str) -> str:
    """Remove palavras comuns do texto."""
    palavras_vazias = [
        "do",
        "de",
        "da",
        "dos",
        "das",
        "dum",
        "duma",
        "dumas",
        "duns",
        "um",
        "uma",
        "uns",
        "umas",
        "a",
        "as",
        "o",
        "os",
        "para",
        "p/",
        "em",
        "na",
        "no",
        "nos",
        "nas",
        "ao",
        "aos",
        "à",
        "às",
    ]
    return " ".join(
        palavra
        for palavra in texto.split()
        if palavra.lower() not in palavras_vazias
    )


tratar_nomes_campos = compose_left(remover_palavras_vazias, normalizar_texto)
