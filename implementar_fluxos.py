# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Implementa fluxos do Prefect no servidor Orion remoto."""


import os
from importlib import import_module
from pathlib import Path
from typing import Final

from dotenv import load_dotenv
from prefect.deployments import Deployment
from prefect.flows import Flow
from prefect.infrastructure.docker import (
    DockerContainer,
    DockerRegistry,
    ImagePullPolicy
)
from prefect.orion.schemas.schedules import CronSchedule

from impulsoetl import __VERSION__ as impulsoetl_versao
from impulsoetl.bd import BD_HOST, BD_PORTA, BD_NOME, BD_USUARIO, BD_SENHA
from impulsoetl.loggers import logger
from impulsoetl.utilitarios.textos import normalizar_texto


logger.info("Lendo configurações a partir de variáveis de ambiente...")
load_dotenv()


# Configurações do registro e da imagem Docker
DOCKER_IMAGEM: Final[str] = os.getenv(
    "DOCKER_IMAGEM",
    "impulsogov/impulsoetl:latest",
)
DOCKER_REGISTRO_SENHA: Final[str] = os.getenv("DOCKER_REGISTRO_SENHA", "")
DOCKER_REGISTRO_URL: Final[str] = os.getenv(
    "DOCKER_REGISTRO_URL",
    "registry.hub.docker.com",
)
DOCKER_REGISTRO_USUARIO: Final[str] = os.getenv("DOCKER_REGISTRO_USUARIO", "")

# Configurações do Prefect
PREFECT_API_URL: Final[str] = os.getenv(
    "PREFECT_API_URL",
    "http://127.0.0.1:4200/api",
)


logger.info("Configurando conexão com registro `{}`...", DOCKER_REGISTRO_URL)
bloco_docker_registro = DockerRegistry(
    username=DOCKER_REGISTRO_USUARIO,
    password=DOCKER_REGISTRO_SENHA,
    registry_url=DOCKER_REGISTRO_URL,
    reauth=True,
)

logger.info("Configurando bloco de infraestrutura com Docker...")
bloco_docker_container = DockerContainer(
    auto_remove=True,
    command=["./entrypoint.sh"],
    env={
        "IMPULSOETL_BD_HOST": BD_HOST,
        "IMPULSOETL_BD_PORTA": str(BD_PORTA),
        "IMPULSOETL_BD_NOME": BD_NOME,
        "IMPULSOETL_BD_USUARIO": BD_USUARIO,
        "IMPULSOETL_BD_SENHA": BD_SENHA,
        "PREFECT_API_URL": PREFECT_API_URL,
    },
    image=DOCKER_IMAGEM,
    image_pull_policy=ImagePullPolicy.ALWAYS,
    image_registry=bloco_docker_registro,
    name="docker-container-impulsoetl",
    stream_output=True,
)

logger.info("Lendo módulos disponíveis...")
geral = import_module(".scripts.geral", "impulsoetl")
impulso_previne = import_module(".scripts.impulso_previne", "impulsoetl")
saude_mental = import_module(".scripts.saude_mental", "impulsoetl")

modulos = [geral, impulso_previne, saude_mental]


if __name__ == "__main__":
    for modulo in modulos:
        modulo_localizacao = Path(modulo.__spec__.origin)
        fila_trabalho = modulo.__name__.split(".")[-1].replace("_", "-")
        fluxos = [
            atributo for atributo in modulo.__dict__.values()
            if isinstance(atributo, Flow)
            # evita importações indiretas
            and atributo.__module__ == modulo.__name__
        ]
        logger.info(
            "Identificados {} fluxos para o módulo `{}`.",
            len(fluxos),
            modulo.__name__,
        )
        for fluxo in fluxos:
            implementacao_nome = normalizar_texto(fluxo.fn.__name__, "-")
            logger.info("Implementando fluxo `{}`...", implementacao_nome)
            implementacao = Deployment.build_from_flow(
                flow=fluxo,
                name=implementacao_nome,
                output=None,
                skip_upload=True,
                apply=False,
                load_existing=True,
                work_queue_name=fila_trabalho,
                infrastructure=bloco_docker_container,
                path=str(modulo_localizacao.parent),
                version=impulsoetl_versao,
            )
            if not implementacao.schedule:
                implementacao.schedule = CronSchedule(
                    cron="0 1 * * *",
                    timezone="America/Sao_Paulo",
                )
            implementacao_id = implementacao.apply(
                upload=False,
                work_queue_concurrency=3,
            )
            logger.info("Fluxo implementado com sucesso!")
            logger.debug(
                "NOME DO FLUXO: {nome} (id {id}). "
                + "VERSÃO: {versao}. "
                + "FILA DE TRABALHO: {fila}. "
                + "IMAGEM: {imagem}",
                nome=implementacao.name,
                id=implementacao_id,
                versao=implementacao.version,
                fila=implementacao.work_queue_name,
                imagem=implementacao.infrastructure.image,
            )
