<!--
SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
-->


# ETL de dados públicos para o banco de dados da ImpulsoGov

Extração, tratamento e caregamento de dados públicos direta ou indiretamente relacionados ao Sistema Único de Saúde brasileiro, tendo como destino o banco de dados da [ImpulsoGov](https://impulsogov.org/).


## Instalação

A instalação do pacote depende do gerenciador de dependências [Poetry][].

[Poetry]: https://python-poetry.org/docs/#installation

```sh
# instalar pré-requisitos do sistema
$ sudo apt-get install gdal-bin libgdal-dev libgdal1h

# clonar e acessar a raíz do repositório
$ git clone https://github.com/ImpulsoGov/etl-dados-publicos.git
$ cd etl-dados-publicos

# Instalar dependências
$ python -m venv .venv
$ source .venv/bin/activate
(.venv) $ python -m pip install GDAL==$(gdal-config --version) --global-option=build_ext --global-option="-I/usr/include/gdal"
(.venv) $ poetry add gdal==$(gdal-config --version)  # Fixar versão do GDAL
(.venv) $ poetry install
```


## Docker

Para criar e rodar a imagem do container atualmente contendo o etl do impulso previne, basta:

```sh
$ docker build -t impulsoprevine .
$ docker run -p 8888:8888 impulsoprevine:latest
```
