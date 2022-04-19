<!--
SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
-->

# ETL de dados públicos para o banco de dados da ImpulsoGov

Extração, tratamento e caregamento de dados públicos direta ou indiretamente relacionados ao Sistema Único de Saúde brasileiro, tendo como destino o banco de dados da [Impulso Gov](https://impulsogov.org/).

## Estrutura do repositório

O repositório possui dois pacotes Python contendo a lógica de para a
captura de dados públicos: `impulsoetl` e `impulsoprevine`.

```plain
etl
├─ src
│  ├─ impulsoetl
│  │  └─ ...
│  └─ impulsoprevine
│     └─ ...
└─ ...   
```

O pacote `impulsoprevine` contém sobretudo dados do SISAB utilizados para consumo pelas aplicações do [Impulso Previne](http://impulsoprevine.com.br/). Esse foi o primeiro pacote de ETL implementado e permanece em uso, mas novas funcionalidades de ETL devem ser implementadas preferencialmente no `impulsoetl`

O pacote `impulsoetl` contém as lógicas de obtenção de dados do SIASUS, SIHSUS e alguns dados do SCNES e do SISAB, incluindo interfaces com agendadores de tarefas e registradores de logs de transações. Inicialmente, foi desenvolvido para a obtenção de dados do [Impulso Saúde Mental](https://impulsosaudemental.org/), mas atualmente esse pacote deve ser preferido para a implementação de todas as funções de ETL - mesmo aquelas relacionadas ao Impulso Previne.

## Instalação via código fonte (para desenvolvimento local)

### ImpulsoETL

A instalação do pacote depende do gerenciador de dependências [Poetry][].

Com o Poetry instalado, em sistemas com gerenciador de pacotes `apt` (ex. Debian, Ubuntu), rode as instruções abaixo no terminal de linha de comando:

[Poetry]: https://python-poetry.org/docs/#installation

```sh
# instalar pré-requisitos do sistema
$ sudo apt-get install gdal-bin libgdal-dev libgdal1h

# clonar e acessar a raíz do repositório
$ git clone https://github.com/ImpulsoGov/etl.git
$ cd etl

# Instalar dependências
$ python -m venv .venv
$ source .venv/bin/activate
(.venv) $ python -m pip install GDAL==$(gdal-config --version) --global-option=build_ext --global-option="-I/usr/include/gdal"
(.venv) $ poetry add gdal==$(gdal-config --version)  # Fixar versão do GDAL
(.venv) $ poetry install -E impulsoetl
```

## Instalação e execução locais com Docker

### ImpulsoETL

Antes de rodar o container com o pacote `impulsoetl` localmente, crie um arquivo nomeado `.env` na raiz do repositório. Esse arquivo deve conter as credenciais de acesso ao banco de dados e outras configurações de execução do ETL. Você pode utilizar o modelo do arquivo `.env.sample` como referência.

Em seguida, execute os comandos abaixo em um terminal de linha de comando (a execução completa pode demorar):

```sh
$ docker build -f impulsoetl.Dockerfile -t impulsoetl
$ docker run -p 8889:8888 impulsoetl:latest
```

Esses comandos vão construir uma cópia local da imagem do Impulso e tentar executar as capturas de dados públicos agendadas no banco de dados.

### Impulso Previne

Para criar e rodar a imagem do container atualmente contendo o ETL do Impulso Previne, execute em um terminal de linha de comando:

```sh
$ docker build -t impulsoprevine .
$ docker run -p 8888:8888 impulsoprevine:latest
```

## Rodando em produção

Tanto o pacote `impulsoetl` quanto o `impulsoprevine` utilizam ações do [GitHub Actions](https://docs.github.com/actions) para enviar imagens para o [DockerHub da Impulso Gov](https://hub.docker.com/orgs/impulsogov/repositories) sempre que há uma atualização da branch principal do repositório. Diariamente, essa imagem é baixada para uma máquina virtual que executa as capturas pendentes.

Para executar os pacotes em produção, defina as credenciais necessárias como [segredos no repositório](https://docs.github.com/en/actions/security-guides/encrypted-secrets). Se necessário, ajuste os arquivos do diretório [.github/actions][./.github/actions] com as definições apropriadas para a execução das tarefas de implantação e de execução dos fluxos de ETL.
