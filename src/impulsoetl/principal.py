# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


from impulsoetl.bd import Sessao
from impulsoetl.scripts.saude_mental import principal as capturas_saude_mental


def principal(teste: bool = False) -> None:
    """Main program entrypoint."""
    with Sessao() as sessao:
        capturas_saude_mental(sessao=sessao, teste=teste)
    # ...outros conjuntos de scripts aqui
