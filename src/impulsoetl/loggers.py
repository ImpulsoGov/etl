# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Define o comportamento dos logs do programa."""


import os
import sys

from loguru import logger
from prefect import get_run_logger
from prefect.exceptions import MissingContextError


_destino = os.getenv("IMPULSOETL_LOG_DESTINO", sys.stderr)
_nivel = os.getenv("IMPULSOETL_LOG_DESTINO", "WARNING")


def habilitar_suporte_loguru() -> None:
    """Redireciona logs do Loguru para o logger do Prefect

    This function should be called from within a Prefect task or flow before calling any module that uses loguru.
    This function can be safely called multiple times.

    Exemplo:

    ```python
    from prefect import flow
    from loguru import logger
    from prefect_utils import enable_loguru_support # import this function in your flow from your module
    @flow()
    def myflow():
        logger.info("This is hidden from the Prefect UI")
        enable_loguru_support()
        logger.info("This shows up in the Prefect UI")
    ```
    """
    # VER https://gist.github.com/anna-geller/0b9e6ecbde45c355af425cd5b97e303d

    try:
        run_logger = get_run_logger()
    except MissingContextError:
        # a função não faz nada se estiver fora do contexto de execução de uma
        # task ou flow
        return

    logger.remove()
    log_format = "{name}:{function}:{line} - {message}"
    logger.add(
        run_logger.debug,
        filter=lambda record: record["level"].name == "DEBUG",
        level="TRACE",
        format=log_format,
    )
    logger.add(
        run_logger.warning,
        filter=lambda record: record["level"].name == "WARNING",
        level="TRACE",
        format=log_format,
    )
    logger.add(
        run_logger.error,
        filter=lambda record: record["level"].name == "ERROR",
        level="TRACE",
        format=log_format,
    )
    logger.add(
        run_logger.critical,
        filter=lambda record: record["level"].name == "CRITICAL",
        level="TRACE",
        format=log_format,
    )
    logger.add(
        run_logger.info,
        filter=lambda record: (
            record["level"].name
            not in ["DEBUG", "WARNING", "ERROR", "CRITICAL"]
        ),
        level="TRACE",
        format=log_format,
    )


logger.add(
    _destino,
    level=_nivel,
    format=(
        "{time:YYYY-MM-DD at HH:mm:ss} | {level} | {file}:{line}: {message}"
    ),
)
