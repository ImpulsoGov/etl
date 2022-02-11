# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Define o comportamento dos logs do programa."""


import os
import sys

from loguru import logger

_destino = os.getenv("IMPULSOETL_LOG_DESTINO", sys.stderr)
_nivel = os.getenv("IMPULSOETL_LOG_DESTINO", "WARNING")

logger.add(
    _destino,
    level=_nivel,
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {file}:{line}: {message}",
)
