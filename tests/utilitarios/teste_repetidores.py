# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Casos de teste para repetidores de funções."""


from datetime import date
from typing import Callable

import pandas as pd
import pytest

from impulsoetl.utilitarios.repetidores import (
    repetir_por_ano_mes,
    repetir_por_uf,
)


class TesteRepetirPorUf(object):
    """Agrupa testes para o decorador `repetir_por_uf()`."""

    @pytest.fixture(scope="class")
    def funcao_por_uf(self) -> Callable[[str], pd.DataFrame]:
        """Retorna uma função para gerar DataFrames de siglas de UF."""

        def funcao(uf: str) -> pd.DataFrame:
            """Retorna um `pandas.DataFrame` de exemplo para uma UF."""
            dados_exemplo = {
                "UF": [uf, uf],
                "variavelA": [1, 2],
                "variavelB": [3, 4],
            }
            return pd.DataFrame(dados_exemplo)

        return funcao

    @pytest.mark.unitario
    def teste_repetir_por_uf_uma(
        self,
        funcao_por_uf: Callable[[int, int], pd.DataFrame],
    ) -> None:
        funcao_decorada = repetir_por_uf(funcao_por_uf)
        dados = funcao_decorada(uf="AC")
        ufs_resultado = list(dados["UF"].unique())
        assert "AC" in ufs_resultado
        assert len(ufs_resultado) == 1

    @pytest.mark.unitario
    def teste_repetir_por_uf_varias(
        self,
        funcao_por_uf: Callable[[int, int], pd.DataFrame],
    ) -> None:
        funcao_decorada = repetir_por_uf(funcao_por_uf)
        dados = funcao_decorada(uf=["AC", "ES"])
        ufs_resultado = list(dados["UF"].unique())
        assert "AC" in ufs_resultado
        assert "ES" in ufs_resultado
        assert "SE" not in ufs_resultado

    @pytest.mark.unitario
    def teste_repetir_por_uf_todas(
        self,
        funcao_por_uf: Callable[[int, int], pd.DataFrame],
    ) -> None:
        funcao_decorada = repetir_por_uf(funcao_por_uf)
        dados = funcao_decorada(uf="BR")
        ufs_resultado = list(dados["UF"].unique())
        for uf in ("AC", "BA", "DF", "ES", "SC"):
            assert uf in ufs_resultado

    @pytest.mark.unitario
    def teste_repetir_por_uf_inexistente(
        self,
        funcao_por_uf: Callable[[int, int], pd.DataFrame],
    ) -> None:
        funcao_decorada = repetir_por_uf(funcao_por_uf)
        with pytest.raises(ValueError):
            funcao_decorada(uf="ZZ")


class TesteRepetirPorAnoMes(object):
    """Agrupa testes para o decorador `repetir_por_ano_mes()`."""

    @pytest.fixture(scope="class")
    def funcao_com_ano_e_mes(self) -> Callable[[int, int], pd.DataFrame]:
        """Retorna uma função para gerar DataFrames de pares de ano e mês."""

        def funcao(ano: int, mes: int) -> pd.DataFrame:
            """Retorna um `pandas.DataFrame` de exemplo para uma UF."""
            data_exemplo = pd.Timestamp(ano, mes, 1)
            dados_exemplo = {
                "data": [data_exemplo, data_exemplo],
                "variavelA": [1, 2],
                "variavelB": [3, 4],
            }
            return pd.DataFrame(dados_exemplo)

        return funcao

    @pytest.mark.unitario
    def teste_repetir_por_ano_mes_um(
        self,
        funcao_com_ano_e_mes: Callable[[int, int], pd.DataFrame],
    ) -> None:
        funcao_decorada = repetir_por_ano_mes(funcao_com_ano_e_mes)
        dados = funcao_decorada(ano=2020, mes=6)
        assert all(
            data.year == 2020 and data.month == 6
            for data in dados["data"]  # noqa: 110
        )

    @pytest.mark.unitario
    def teste_repetir_por_intervalo_valido(
        self,
        funcao_com_ano_e_mes: Callable[[int, int], pd.DataFrame],
    ) -> None:
        funcao_decorada = repetir_por_ano_mes(funcao_com_ano_e_mes)
        dados = funcao_decorada(
            data_inicio="2020-01-01",
            data_fim="2020-12-31",
        )
        assert all(
            data.year == 2020 and data.month in list(range(1, 13))
            for data in dados["data"]  # noqa: 110
        ), "A função decorada retornou um data inesperada."
        assert all(
            mes in list(dados["data"].apply(lambda dt: dt.month))
            for mes in range(1, 13)
        ), "A função decorada não retornou todos os meses do intervalo."

    @pytest.mark.unitario
    @pytest.mark.parametrize(
        "data_inicio",
        ["2020-01-18", pd.Timestamp(2021, 3, 1), date(2008, 12, 30)],
    )
    def teste_repetir_por_intervalo_invalido(
        funcao_com_ano_e_mes,
        data_inicio,
    ) -> None:
        # data de fim inferior à de início
        data_fim = pd.Timestamp(data_inicio) - pd.Timedelta(days=30)
        funcao_decorada = repetir_por_ano_mes()(funcao_com_ano_e_mes)
        with pytest.raises(ValueError):
            funcao_decorada(data_inicio=data_inicio, data_fim=data_fim)

    @pytest.mark.unitario
    def teste_repetir_por_intervalo_antes_minimo(funcao_com_ano_e_mes) -> None:
        # data de fim inferior à de início
        funcao_decorada = repetir_por_ano_mes(
            data_inicio_minima="2020-01-01",
        )(funcao_com_ano_e_mes)
        with pytest.raises(ValueError):
            funcao_decorada(data_inicio="2019-01-01", data_fim="2020-12-01")
