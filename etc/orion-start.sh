#!/bin/bash

# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


# mova este arquivo para o diretório /home/prefect/
# Permita a execução dele rodando o comando `chmod +x /home/prefect/orion-start.sh`

source "/home/prefect/.venv/bin/activate"
ipv4="$(dig -4 TXT +short o-o.myaddr.l.google.com @ns1.google.com)"
ipv4="${ipv4%\"}"
ipv4="${ipv4#\"}"
prefect config set PREFECT_ORION_UI_API_URL="http://${ipv4%\"#\"}/api"
prefect orion start --host 0.0.0.0

