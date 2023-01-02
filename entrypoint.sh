#!/bin/bash

# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

set -e

exec /home/appuser/.local/bin/poetry run python3 -m prefect.engine "$@"
