# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Define categorias de datas e períodos utilizadas em vários processos de ETL.
"""


from __future__ import annotations

import re
from typing import Final


CID10: Final[re.Pattern] = re.compile(
    r"[A-Z][0-9]{2}\.?[0-9X]{,4}",
    re.IGNORECASE,
)


def e_cid10(texto: str) -> bool:
    """Indica se um texto fornecido é compatível com o padrão da CID-10."""
    return bool(CID10.match(texto))
