# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


from impulsoetl.bd import Sessao
from impulsoetl.scripts.geral import principal as capturas_uso_geral
from impulsoetl.scripts.impulso_previne import (
    principal as capturas_impulso_previne,
)
from impulsoetl.scripts.saude_mental import principal as capturas_saude_mental


def principal(teste: bool = False) -> None:
    """Main program entrypoint."""
    with Sessao() as sessao:
        capturas_uso_geral(sessao=sessao, teste=teste)
        capturas_saude_mental(sessao=sessao, teste=teste)
        capturas_impulso_previne(sessao=sessao, teste=teste)
    # ...outros conjuntos de scripts aqui
