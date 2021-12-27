# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
# SPDX-License-Identifier: MIT

FROM python:3.8.9

# Configurar variáveis de ambiente
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1
ENV POETRY_VIRTUALENVS_CREATE 0
ENV POETRY_VIRTUALENVS_IN_PROJECT 0
ENV IMPULSOETL_AMBIENTE "producao"

# atualizar repositórios
RUN apt-get update

# # Instalar Google Chrome
# RUN wget --no-check-certificate -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
# RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
# RUN apt-get update
# RUN apt-get install -yqq google-chrome-stable

# # Instalar Chromedriver
# RUN apt-get install -yqq unzip
# RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
# RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

# Instalar Geckodriver e Firefox-ESR
RUN wget -O /tmp/geckodriver.zip \
    https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux32.tar.gz
RUN tar -xf /tmp/geckodriver.zip -C /usr/local/bin/
RUN apt-get install -yqq firefox-esr

# Instalar GDAL
RUN apt-get install -yqq gdal-bin libgdal-dev libgeos-dev

# instalar dependências do pacote cryptography
RUN apt-get install -yqq build-essential libssl-dev libffi-dev python3-dev cargo

# Criar e logar em novo usuário; copiar arquivos e configurar suas permissões
RUN useradd --create-home appuser
WORKDIR /home/appuser
COPY . .
RUN chown -R appuser /home/appuser
USER appuser

# Instalar Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/home/appuser/.local/bin:$PATH"

# Instalar dependências Python
RUN python3 -m pip install --upgrade pip
# GDAL e cryptography são dependências problemáticas; precisam ser instaladas
# separadamente com pip
RUN python3 -m pip install GDAL==$(gdal-config --version) \
    --global-option=build_ext --global-option="-I/usr/include/gdal"
RUN poetry add "GDAL==$(gdal-config --version)" --lock
# gambiarra enquanto poetry não oferece opção --no-binary; 
# ver https://github.com/python-poetry/poetry/issues/365#issuecomment-792054069
# (nesse caso, o poetry funciona apenas para melhorar a resolução de dependências)
RUN poetry export \
    -E impulsoetl \
    --without-hashes \
    --format requirements.txt \
    --output requirements.txt \
    && sed -i -e 's/^-e //g' requirements.txt
RUN CARGO_NET_GIT_FETCH_WITH_CLI=true \
    python -m pip install \
    --force-reinstall \
    --no-binary cryptography \
    -r requirements.txt

# instalar pacote impulsoetl
RUN poetry build
RUN python3 -m pip install --no-index --find-links="./dist" impulsoetl

# Executar o ponto de entrada contendo os scripts
CMD [ "python3", "src/impulsoetl/__main__.py"]