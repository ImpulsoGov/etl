#!/bin/bash

# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


# mova este arquivo para o diretório /home/prefect/
# Permita a execução dele rodando o comando `chmod +x /home/prefect/agent-start.sh`

source "/home/prefect/.venv/bin/activate"
source "/home/prefect/.env"
ipv4="$(dig -4 TXT +short o-o.myaddr.l.google.com @ns1.google.com)"
ipv4="${ipv4%\"}"
ipv4="${ipv4#\"}"
prefect config set PREFECT_API_URL="http://${PREFECT_API_USUARIO}:${PREFECT_API_SENHA}@${ipv4%\"#\"}/api"
prefect agent start -q impulso-previne -q saude-mental -q geral

