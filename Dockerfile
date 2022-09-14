# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
# SPDX-License-Identifier: MIT

FROM python:3.8.13-slim-bullseye

# Configurar variáveis de ambiente
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1
ENV POETRY_VIRTUALENVS_CREATE 1
ENV POETRY_VIRTUALENVS_IN_PROJECT 0
ENV IMPULSOETL_AMBIENTE "producao"

# atualizar repositórios
RUN apt-get clean all -qq
RUN apt-get update -yqq
RUN apt-get dist-upgrade -yqq
RUN apt-get autoremove -yqq
RUN python3 -m pip install --upgrade pip

# Instalar Geckodriver e Firefox-ESR
RUN apt-get install -yqq curl
RUN curl -s -L \
    https://github.com/mozilla/geckodriver/releases/download/v0.31.0/geckodriver-v0.31.0-linux64.tar.gz \
    | tar xz -C /usr/local/bin/
RUN apt-get install -yqq firefox-esr:amd64=91.13.0esr-1~deb11u1	

# instalar dependências
RUN apt-get install -yqq git build-essential libssl-dev libffi-dev python3-dev cargo

# Criar e logar em novo usuário; copiar configurações e atribuir permissões
RUN useradd --create-home appuser
WORKDIR /home/appuser
COPY pyproject.toml ./pyproject.toml
COPY poetry.lock ./poetry.lock
RUN chown -R appuser /home/appuser
USER appuser

# Instalar Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/home/appuser/.local/bin:$PATH"

# Instalar dependências Python
RUN poetry install --no-dev --no-root

# copiar código-fonte e instalar pacote impulsoetl
COPY README.md ./README.md
COPY src ./src
RUN poetry install --no-dev

# Executar o ponto de entrada contendo os scripts
CMD [ "poetry", "run", "python3", "-m", "impulsoetl"]
