# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


def checar_codigos(
    serie: pd.Series,
    codigos_alvo: List[str],
    varios_codigos_por_celula: bool = False,
) -> pd.Series:
    """Verifica se há elementos de uma lista de códigos em uma `pd.Series`."""

    if varios_codigos_por_celula:
        return serie.apply(
            lambda elemento: any(
                [elemento.startswith(cod) for cod in codigos_alvo],
            ),
        )

    # se a série contiver vários códigos em uma mesma célula, concatenados
    # por algum caractere alfanumérico, utilizar o módulo `re` para
    # verificar se algum se aplica (mais lento)
    return serie.apply(
        lambda elemento: any(
            [
                re.search(r"\W{}".format(cod), elemento, re.ASCII)
                for cod in codigos_alvo
            ],
        ),
    )
