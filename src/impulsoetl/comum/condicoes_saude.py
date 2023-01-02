# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Define categorias de datas e períodos utilizadas em vários processos de ETL.
"""


import re
from typing import Final


CID10: Final[re.Pattern] = re.compile(
    r"[A-Z][0-9]{2}\.?[0-9X]{,4}",
    re.IGNORECASE,
)


def e_cid10(texto: str) -> bool:
    """Indica se um texto fornecido é compatível com o padrão da CID-10."""
    return bool(len(texto) > 2 and len(texto) < 8 and CID10.match(texto))


def remover_ponto_cid10(texto: str) -> bool:
    """Remove caractere de ponto após o 3º dígito de um código CID-10."""
    return re.sub(r"([A-Z][0-9]{2})\.?([0-9X]{,4})", r"\1\2", texto)
