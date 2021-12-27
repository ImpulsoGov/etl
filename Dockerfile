# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
# SPDX-License-Identifier: MIT

# Estou colocando aqui uma imagem simples, para discutirmos no 
# futuro uma estrutura apropriada pra o container
FROM python:3.8

ENV SLACK_WEBHOOK_URL="https://hooks.slack.com/services/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" \
    IS_LOCAL=FALSE \
    IS_PROD=TRUE

WORKDIR /app
COPY . .

# install google chrome
RUN apt update -y
RUN wget --no-check-certificate -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable
# install chromedriver
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

COPY ./src/impulsoprevine .
RUN pip install -r requirements.txt
CMD [ "sh", "entrypoint.sh"]
# CMD [ "python", "./src/impulsoprevine/main.py"]




# FROM python:3.9-slim AS base

# # Setup env
# ENV LANG C.UTF-8
# ENV LC_ALL C.UTF-8
# ENV PYTHONDONTWRITEBYTECODE 1
# ENV PYTHONFAULTHANDLER 1


# FROM base AS python-deps

# # Install poetry and compilation dependencies
# RUN apt-get install gdal-bin libgdal-dev libgdal1h
# RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
# RUN apt-get update && apt-get install -y --no-install-recommends gcc

# # Install python dependencies in /.venv
# COPY pyproject.toml .
# COPY poetry.lock .
# RUN python -m venv .venv
# RUN source .venv/bin/activate
# RUN python -m pip install GDAL==$(gdal-config --version) --global-option=build_ext --global-option="-I/usr/include/gdal"
# RUN POETRY_VIRTUALENVS_IN_PROJECT=1 poetry install --no-dev


# FROM base AS runtime

# # Copy virtual env from python-deps stage
# COPY --from=python-deps /.venv /.venv
# ENV PATH="/.venv/bin:$PATH"

# # Create and switch to a new user
# RUN useradd --create-home appuser
# WORKDIR /home/appuser
# USER appuser

# # Install application into container
# COPY . .

# # Run the executable
# ENTRYPOINT ["python", "-m", "impulsoetl"]