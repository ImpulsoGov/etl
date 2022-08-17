<!--
SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
-->

# ETL de dados públicos para o banco de dados da ImpulsoGov

Extração, tratamento e caregamento de dados públicos direta ou indiretamente relacionados ao Sistema Único de Saúde brasileiro, tendo como destino o banco de dados da [Impulso Gov](https://impulsogov.org/).

## Estrutura do repositório

O repositório possui um pacote Python contendo a lógica para a
captura de dados públicos, sob o nome `impulsoetl`.

```plain
etl
├─ src
│  ├─ impulsoetl
│  │  └─ ...
└─ ...
```

O pacote `impulsoetl` contém as lógicas de obtenção de dados do SIASUS, SIHSUS
e alguns dados do SCNES e do SISAB, incluindo interfaces com agendadores de
tarefas e registradores de logs de transações.

## Instalação via código fonte (para desenvolvimento local)

A instalação do pacote depende do gerenciador de dependências [Poetry][].

Com o Poetry instalado, em sistemas com gerenciador de pacotes `apt` (ex. Debian, Ubuntu), rode as instruções abaixo no terminal de linha de comando:

[Poetry]: https://python-poetry.org/docs/#installation

```sh
# clonar e acessar a raíz do repositório
$ git clone https://github.com/ImpulsoGov/etl.git
$ cd etl

# Instalar pacote e dependências
$ poetry install -E impulsoetl
```

## Instalação e execução locais com Docker

Antes de rodar o container com o pacote `impulsoetl` localmente, crie um arquivo nomeado `.env` na raiz do repositório. Esse arquivo deve conter as credenciais de acesso ao banco de dados e outras configurações de execução do ETL. Você pode utilizar o modelo do arquivo `.env.sample` como referência.

Em seguida, execute os comandos abaixo em um terminal de linha de comando (a execução completa pode demorar):

```sh
$ docker build -t impulsoetl .
$ docker run -p 8888:8888 impulsoetl:latest
```

Esses comandos vão construir uma cópia local da imagem do Impulso e tentar executar as capturas de dados públicos agendadas no banco de dados.

## Rodando em produção

O pacote `impulsoetl` utiliza ações do
[GitHub Actions](https://docs.github.com/actions) para enviar imagens para o
[DockerHub da Impulso Gov](https://hub.docker.com/orgs/impulsogov/repositories)
sempre que há uma atualização da branch principal do repositório. Diariamente,
essa imagem é baixada para uma máquina virtual que executa as capturas
pendentes.

Para executar os pacotes em produção, defina as credenciais necessárias como [segredos no repositório](https://docs.github.com/en/actions/security-guides/encrypted-secrets). Se necessário, ajuste os arquivos do diretório [.github/workflows](./.github/workflows) com as definições apropriadas para a execução das tarefas de implantação e de execução dos fluxos de ETL.
