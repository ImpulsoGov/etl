# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Testes para obter relatórios de contatos assistenciais na atenção primária.
"""


import re
from copy import deepcopy
from datetime import date

import pandas as pd
import pytest
from selenium.webdriver import Firefox
from sqlalchemy import Column, Text
from sqlalchemy.orm import DeclarativeMeta

from impulsoetl.bd import Base
from impulsoetl.navegadores import criar_geckodriver
from impulsoetl.sisab.excecoes import SisabErroRotuloOuValorInexistente
from impulsoetl.sisab.modelos import TabelaProducao
from impulsoetl.sisab.producao import (
    ConsultaProducaoMetadados,
    FormularioConsulta,
    RelatorioProducao,
    carregar_relatorio_producao,
    gerar_modelo_impulso,
    gerar_nome_tabela,
    obter_relatorio_producao,
)
from impulsoetl.utilitarios.textos import tratar_nomes_campos


@pytest.fixture(scope="module")
def driver() -> Firefox:
    with criar_geckodriver() as geckodriver:
        yield geckodriver


@pytest.fixture(scope="function")
def relatorio_transformado() -> pd.DataFrame:
    """Exemplo de relatório de produção transformado a partir do SISAB."""
    colunas = [
        "tipo_equipe",
        "tipo_producao",
        "categoria_profissional",
        "quantidade_registrada",
        "unidade_geografica_id",
        "periodo_id",
    ]
    dados = [
        (
            "Eq. AB Prisional - EABp",
            "Atendimento Individual",
            "Profissional de educação física",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "Eq. Ag. Com. de Saúde - EACS",
            "Atendimento Individual",
            "Profissional de educação física",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "Eq. Consultório na Rua - ECR",
            "Atendimento Individual",
            "Profissional de educação física",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "Eq. da Atenção Básica - EAB",
            "Atendimento Individual",
            "Profissional de educação física",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "Eq. de Atenção Primária - eAP",
            "Atendimento Individual",
            "Profissional de educação física",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "Eq. de Saúde Bucal - SB",
            "Atendimento Individual",
            "Profissional de educação física",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "Eq. de Saúde da Família  - ESF",
            "Atendimento Individual",
            "Profissional de educação física",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "NASF",
            "Atendimento Individual",
            "Profissional de educação física",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "Eq. AB Prisional - EABp",
            "Atendimento Individual",
            "Educador social",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "Eq. Ag. Com. de Saúde - EACS",
            "Atendimento Individual",
            "Educador social",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "Eq. Consultório na Rua - ECR",
            "Atendimento Individual",
            "Educador social",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "Eq. da Atenção Básica - EAB",
            "Atendimento Individual",
            "Educador social",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "Eq. de Atenção Primária - eAP",
            "Atendimento Individual",
            "Educador social",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "Eq. de Saúde Bucal - SB",
            "Atendimento Individual",
            "Educador social",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "Eq. de Saúde da Família  - ESF",
            "Atendimento Individual",
            "Educador social",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            "NASF",
            "Atendimento Individual",
            "Educador social",
            0,
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
    ]
    return pd.DataFrame(data=dados, columns=colunas)


@pytest.fixture(scope="session")
def modelo_categoria_profissional_por_tipo_equipe() -> object:
    class ModeloTabela(TabelaProducao, Base):
        __table_args__ = {"keep_existing": True}
        __tablename__ = (
            "dados_publicos."
            + "sisab_producao_municipios_por_categoria_profissional_por_tipo_e"
        )

        categoria_profissional = Column("categoria_profissional", Text)
        tipo_equipe = Column("tipo_equipe", Text)

    return ModeloTabela


class TesteFiltroCiapCid(object):
    """Testa filtro de CIAP e CID."""

    @pytest.fixture(scope="class")
    def filtro_ciap_cid(self, driver):
        return FormularioConsulta(
            driver,
            tipo_producao="Atendimento Individual",
            competencias=[pd.Timestamp(2021, 9, 1)],
        ).ciap_cid

    @pytest.fixture(autouse=True)
    def limpar_ciap_cid(self, filtro_ciap_cid) -> None:
        """Limpa filtros de CIAP e CID."""
        filtro_ciap_cid.limpar_ciaps_cids()

    def teste_listar_cids_disponiveis(self, filtro_ciap_cid):
        """Testa listar os CIDs disponíveis no formulário de produção."""
        cids_disponiveis = filtro_ciap_cid.cids_disponiveis
        assert (
            "F00" in cids_disponiveis
        ), "CID F00 (Demência Na Doença De Alzheimer) não encontrado!"
        assert (
            "F200" in cids_disponiveis
        ), "CID F200 (Esquizofrenia Paranoide) não encontrado!"

    def teste_listar_ciaps_disponiveis(self, filtro_ciap_cid):
        """Testa listar os CIDs disponíveis no formulário de produção."""
        ciaps_disponiveis = filtro_ciap_cid.ciaps_disponiveis
        assert (
            "58" in ciaps_disponiveis
        ), "CIAP 58 (Aconselhamento/Escuta Terapêutica) não encontrado!"
        assert (
            "P76" in ciaps_disponiveis
        ), "CIAP P76 (Perturbações Depressivas) não encontrado!"

    @pytest.mark.parametrize("ciap", ["58", "P75"])
    def teste_adicionar_ciap_correto(self, filtro_ciap_cid, ciap):
        """Testa adicionar um CIAP válido ao relatório."""
        filtro_ciap_cid.adicionar_ciap(ciap)
        ciaps_ativos = filtro_ciap_cid.listar_ciaps_ativos()
        assert ciap in ciaps_ativos

    @pytest.mark.parametrize("ciap", ["foo"])
    def teste_adicionar_ciap_errado(self, filtro_ciap_cid, ciap):
        """Testa adicionar um CIAP inválido ao relatório."""
        with pytest.raises(SisabErroRotuloOuValorInexistente):
            filtro_ciap_cid.adicionar_ciap(ciap)

    @pytest.mark.parametrize("cid", ["F00", "F200"])
    def teste_adicionar_cid_correto(self, filtro_ciap_cid, cid):
        """Testa adicionar um CID válido ao relatório."""
        filtro_ciap_cid.adicionar_cid(cid)
        cids_ativos = filtro_ciap_cid.listar_cids_ativos()
        assert cid in cids_ativos

    @pytest.mark.parametrize("cid", ["foo"])
    def teste_adicionar_cid_errado(self, filtro_ciap_cid, cid):
        """Testa adicionar um CID inválido ao relatório."""
        with pytest.raises(SisabErroRotuloOuValorInexistente):
            filtro_ciap_cid.adicionar_cid(cid)

    @pytest.mark.integracao
    def teste_adicionar_todos_ciaps(self, filtro_ciap_cid):
        """Testa adicionar todos os CIAPs possíveis ao relatório."""
        ciaps_disponiveis = filtro_ciap_cid.ciaps_disponiveis
        for ciap in ciaps_disponiveis:
            filtro_ciap_cid.adicionar_ciap(ciap)

        ciaps_ativos = filtro_ciap_cid.listar_ciaps_ativos()
        assert "58" in ciaps_ativos
        assert "P76" in ciaps_ativos

    @pytest.mark.integracao
    def teste_adicionar_todos_cids(self, filtro_ciap_cid):
        """Testa adicionar todos os CIDs possíveis ao relatório."""
        cids_disponiveis = filtro_ciap_cid.cids_disponiveis
        for cid in cids_disponiveis:
            filtro_ciap_cid.adicionar_cid(cid)

        cids_ativos = filtro_ciap_cid.listar_cids_ativos()
        assert "F00" in cids_ativos
        assert "F200" in cids_ativos


class TesteFormularioConsulta(object):
    """Testa a manipulação e envio de formulários de consulta da produção."""

    def teste_iniciar_pagina(self, driver):
        formulario = FormularioConsulta(driver)
        assert "SISAB" in formulario.driver.title
        assert not formulario.competencias.ler_competencias_ativas()
        assert formulario.unidade_geografica.ler_rotulo_ativo() == "Brasil"
        assert formulario.linha_relatorio.ler_rotulo_ativo() == "Brasil"
        assert formulario.coluna_relatorio.ler_rotulo_ativo() == (
            "Tipo de Produção"
        )

    @pytest.mark.parametrize(
        "competencias",
        [
            [pd.Timestamp(2021, 8, 1)],
            [date(2020, 12, 1)],
            pd.date_range(start=date(2021, 1, 1), periods=3, freq="MS"),
        ],
    )
    def teste_definir_competencias(self, driver, competencias):
        """Testa selecionar diferentes competências no formulário."""
        formulario = FormularioConsulta(
            driver,
            competencias=competencias,
        )
        competencias_dadas = sorted(competencias)
        competencias_definidas = sorted(
            formulario.competencias.ler_competencias_ativas(),
        )
        assert len(competencias_definidas) == len(competencias_dadas)
        for dada, definida in zip(competencias_dadas, competencias_definidas):
            assert pd.Timestamp(dada) == definida

    @pytest.mark.parametrize("linha,coluna", [("Estado", "Competência")])
    def teste_mudar_linhas_colunas(self, driver, linha, coluna):
        """Testa alterar as definições de linhas e colunas de um relatório."""
        formulario = FormularioConsulta(
            driver,
            linha_relatorio=linha,
            coluna_relatorio=coluna,
        )
        assert formulario.linha_relatorio.ler_rotulo_ativo() == linha
        assert formulario.coluna_relatorio.ler_rotulo_ativo() == coluna

    @pytest.mark.parametrize(
        "unidade_geografica,estado,municipios",
        [("Municípios", "SE", ["280030"])],
    )
    def teste_mudar_unidade_geografica(
        self,
        driver,
        unidade_geografica,
        estado,
        municipios,
    ):
        """Testa alterar as definições de linhas e colunas de um relatório."""
        formulario = FormularioConsulta(
            driver,
            unidade_geografica=unidade_geografica,
            estado=estado,
            municipios=municipios,
        )

        assert (
            formulario.unidade_geografica.ler_rotulo_ativo()
            == unidade_geografica
        )
        assert formulario.estado.ler_rotulo_ativo() == estado

        municipios_definidos = formulario.municipios.ler_valores_ativos()
        assert len(municipios_definidos) == len(municipios)
        for municipio in municipios:
            assert municipio in municipios_definidos

    @pytest.mark.parametrize("competencias", [[pd.Timestamp(2021, 8, 1)]])
    def teste_baixar_relatorio(self, driver, competencias):
        """Testa baixar um relatório simples, com valores padrão."""
        formulario = FormularioConsulta(
            driver,
            competencias=competencias,
        )
        formulario.executar_consulta()
        relatorio_cru = formulario._resultado
        assert relatorio_cru, "Relatório vazio."
        assert isinstance(relatorio_cru, str), "Tipo incorreto p/ o relatório."
        assert relatorio_cru.count("\n") > 5, "Relatório corrompido"
        assert "Atendimento Individual" in relatorio_cru
        assert "Atendimento Odontológico" in relatorio_cru
        assert "Procedimento" in relatorio_cru
        assert "Visita Domiciliar" in relatorio_cru

    @pytest.mark.integracao
    @pytest.mark.parametrize(
        "competencias,linha_relatorio,coluna_relatorio,unidade_geografica,"
        + "estado,municipios,tipo_producao,problema_condicao_avaliada,conduta",
        [
            (
                [pd.Timestamp(2021, 9, 1)],  # competencias
                "Competência",  # linha_relatorio
                "Conduta",  # coluna_relatorio
                "Municípios",  # unidade_geografica
                "SE",  # estado
                ["280030"],  # municipios
                "Atendimento Individual",  # tipo_producao
                # problema_condicao_avaliada
                [
                    "Usuário de álcool",
                    "Usuário de outras drogas",
                    "Saúde mental",
                ],
                ["Selecionar Todos"],  # conduta
            ),
        ],
    )
    def teste_baixar_atendimentos_individuais(
        self,
        driver,
        competencias,
        linha_relatorio,
        coluna_relatorio,
        unidade_geografica,
        estado,
        municipios,
        tipo_producao,
        problema_condicao_avaliada,
        conduta,
    ):
        """Testa baixar um relatório complexo do SISAB."""

        formulario = FormularioConsulta(
            driver,
            competencias=competencias,
            linha_relatorio=linha_relatorio,
            coluna_relatorio=coluna_relatorio,
            unidade_geografica=unidade_geografica,
            estado=estado,
            municipios=municipios,
            tipo_producao=tipo_producao,
            problema_condicao_avaliada=problema_condicao_avaliada,
            conduta=conduta,
        )

        formulario.executar_consulta()
        relatorio_cru = formulario._resultado

        assert relatorio_cru, "Relatório vazio"
        assert isinstance(relatorio_cru, str)
        assert relatorio_cru.count("\n") > 5, "Relatório corrompido"
        assert "Usuário de álcool" in relatorio_cru
        assert "ARACAJU" in relatorio_cru


class TesteRelatorioProducao(object):
    """Testa aplicar transformações no formato dos dados obtidos do SISAB."""

    @pytest.fixture(scope="class")
    def formulario_atendimentos_individuais(
        self,
        driver,
    ) -> FormularioConsulta:
        """Formulário de consulta de atendimentos em saúde mental em Aracaju."""
        formulario = FormularioConsulta(
            driver,
            competencias=[pd.Timestamp(2021, 9, 1)],
            linha_relatorio="Tipo de Equipe",
            coluna_relatorio="Categoria Profissional",
            unidade_geografica="Municípios",
            estado="SE",
            municipios=["280030"],  # Aracaju
            tipo_producao="Atendimento Individual",
            tipo_equipe=["Selecionar Todos"],
            categoria_profissional=["Selecionar Todos"],
        )
        formulario.executar_consulta()
        return formulario

    @pytest.fixture(scope="class")
    def relatorio_atendimentos_individuais(
        self,
        formulario_atendimentos_individuais,
    ) -> RelatorioProducao:
        """Relatório de produção de atendimentos individuais."""
        return formulario_atendimentos_individuais.resultado

    @pytest.fixture(scope="function")
    def relatorio_dados(
        self,
        relatorio_atendimentos_individuais,
    ) -> RelatorioProducao:
        """Dados de produção de atendimentos individuais."""
        return relatorio_atendimentos_individuais.dados.copy()

    def teste_instanciar_relatorio_producao(
        self,
        formulario_atendimentos_individuais,
    ):
        """Testa instanciar um relatório produção."""
        resultado = formulario_atendimentos_individuais.resultado
        assert isinstance(resultado, RelatorioProducao)
        assert getattr(resultado, "dados") is not None
        assert getattr(resultado, "metadados_consulta") is not None
        assert isinstance(resultado.dados, pd.DataFrame)
        assert isinstance(
            resultado.metadados_consulta,
            ConsultaProducaoMetadados,
        )

    def teste_remover_colunas_sobressalentes(
        self,
        relatorio_atendimentos_individuais,
        relatorio_dados,
    ) -> pd.DataFrame:
        """Testa remover colunas sem nome."""
        assert any(relatorio_dados.columns.str.match("Unnamed"))
        df = (
            relatorio_atendimentos_individuais._remover_colunas_sobressalentes(
                relatorio_dados,
            )
        )
        assert not any(df.columns.str.match("Unnamed"))

    def teste_repor_nomes_colunas_truncados(
        self,
        relatorio_atendimentos_individuais,
        relatorio_dados,
    ):
        """Testa restaurar caracteres truncados em nomes das colunas."""
        df = relatorio_atendimentos_individuais._repor_nomes_colunas_truncados(
            relatorio_dados,
        )
        colunas = df.columns

        assert "Técnico e auxiliar de enfermagem" in colunas

    def teste_relatorio_verticalizar_colunas(
        self,
        relatorio_atendimentos_individuais,
        relatorio_dados,
    ):
        """Testa remodelar valores em colunas de um DataFrame para linhas."""
        colunas_nomes_curtos = [
            col
            for col in relatorio_dados
            if len(col) != 30 and not col.startswith("Unnamed")
        ]
        df = relatorio_atendimentos_individuais._verticalizar_colunas(
            relatorio_dados.loc[:, colunas_nomes_curtos],
        )
        colunas = df.columns

        assert "Categoria Profissional" in colunas
        assert (
            "Agente comunitário de saúde"
            in df["Categoria Profissional"].unique()
        )
        assert "quantidade_registrada" in colunas

    def teste_adicionar_filtros_como_colunas(
        self,
        relatorio_atendimentos_individuais,
        relatorio_dados,
    ):
        """Testa inserir campos referentes aos filtros utilizados na consulta."""
        df = (
            relatorio_atendimentos_individuais._adicionar_filtros_como_colunas(
                relatorio_dados,
            )
        )
        colunas = df.columns

        assert "competencias" in colunas
        assert "municipios" in colunas
        assert "tipo_producao" in colunas

        assert all(
            competencia == pd.Timestamp(2021, 9, 1)
            for competencia in df["competencias"]
        )
        assert all(
            unidade_geografica_id == "280030"
            for unidade_geografica_id in df["municipios"]
        )
        assert all(
            tipo_producao == "Atendimento Individual"
            for tipo_producao in df["tipo_producao"]
        )

    def teste_normalizar_nomes_colunas(self, relatorio_dados):
        """Testa normalizar nomes das colunas."""
        relatorio_dados["municipios"] = "280030"
        df = RelatorioProducao._normalizar_nomes_colunas(relatorio_dados)
        colunas = df.columns

        assert "tipo_equipe" in colunas
        assert "municipio_id_sus" in colunas

    def teste_impor_tipos(self, relatorio_dados):
        """Testa impor tipos de dados para as colunas."""
        relatorio_dados["municipio_id_sus"] = 280030
        relatorio_dados["quantidade_registrada"] = relatorio_dados.sum(
            axis=0,
            skipna=False,
            numeric_only=True,
        )
        df = RelatorioProducao._impor_tipos(relatorio_dados)
        tipos = df.dtypes.to_dict()
        assert tipos["municipio_id_sus"] == "object"
        assert tipos["quantidade_registrada"] == "Int64"

    def teste_aplicar_ids_impulso(
        self,
        relatorio_atendimentos_individuais,
        relatorio_dados,
        sessao,
    ):
        """Testa substituir colunas pelos IDs do BD da Impulso."""
        relatorio_dados["municipio_id_sus"] = "280030"
        relatorio_dados["competencias"] = pd.Timestamp(2021, 9, 1)
        assert "unidade_geografica_id" not in relatorio_dados.columns
        assert "periodo_id" not in relatorio_dados.columns
        relatorio_atendimentos_individuais.sessao = sessao
        df = relatorio_atendimentos_individuais._aplicar_ids_impulso(
            relatorio_dados,
        )
        colunas = df.columns
        assert "unidade_geografica_id" in colunas
        assert "periodo_id" in colunas
        assert "municipio_id_sus" not in colunas
        assert "competencias" not in colunas

    @pytest.mark.integracao
    def teste_aplicar_transformacoes(
        self,
        relatorio_atendimentos_individuais,
        sessao,
    ):
        """Testa padronizar dados de relatórios de produção."""
        relatorio_atendimentos_individuais.sessao = None
        relatorio = deepcopy(relatorio_atendimentos_individuais)
        relatorio_atendimentos_individuais.sessao = sessao
        relatorio.sessao = sessao

        relatorio.aplicar_transformacoes()

        colunas = relatorio.dados.columns

        assert "tipo_producao" in colunas
        assert "categoria_profissional" in colunas
        assert "tipo_equipe" in colunas
        assert "unidade_geografica_id" in colunas
        assert "periodo_id" in colunas
        assert "quantidade_registrada" in colunas


@pytest.mark.parametrize(
    "variaveis,unidade_geografica,nome_esperado",
    [
        (
            ["Tipo de Equipe", "Categoria do Profissional"],
            "Municípios",
            "dados_publicos.sisab_producao_municipios_por_categoria"
            + "_profissional_por_tipo_e",
        ),
    ],
)
def teste_gerar_nome_tabela(variaveis, unidade_geografica, nome_esperado):
    nome_gerado = gerar_nome_tabela.fn(
        variaveis=variaveis,
        unidade_geografica=unidade_geografica,
    )
    assert nome_gerado == nome_esperado


@pytest.mark.parametrize(
    "variavel_a,variavel_b",
    [
        ("Tipo de Equipe", "Categoria do Profissional"),
        ("Conduta", "Problema/Condição Avaliada"),
    ],
)
def teste_gerar_modelo_impulso(variavel_a, variavel_b, relatorio_transformado):
    """Testa gerar modelo de relatório de produção."""

    tabela_nome = gerar_nome_tabela.fn(
        variaveis=(variavel_a, variavel_b),
        unidade_geografica="municipios",
    )
    modelo = gerar_modelo_impulso.fn(
        tabela_nome=tabela_nome,
        variaveis=(variavel_a, variavel_b),
    )

    assert isinstance(modelo, DeclarativeMeta)
    assert issubclass(modelo, TabelaProducao)
    assert issubclass(modelo, Base)
    assert modelo.__tablename__.startswith("sisab_producao_municipios")
    assert hasattr(modelo, "unidade_geografica_id")
    assert hasattr(modelo, "periodo_id")
    assert hasattr(modelo, tratar_nomes_campos(variavel_a))
    assert hasattr(modelo, tratar_nomes_campos(variavel_b))


def teste_carregar_relatorio_producao(
    sessao,
    relatorio_transformado,
    modelo_categoria_profissional_por_tipo_equipe,
    capfd,
):
    """Testa carregar relatório de produção na base de dados da ImpulsoGov."""
    nome_esperado = (
        "dados_publicos"
        + ".sisab_producao_municipios_por_categoria_profissional_por_tipo_e"
    )
    linhas_esperadas = 16

    codigo_saida = carregar_relatorio_producao.fn(
        sessao=sessao,
        dados_producao=relatorio_transformado,
        modelo_tabela=(modelo_categoria_profissional_por_tipo_equipe),
    )

    assert codigo_saida == 0

    logs = capfd.readouterr().err
    assert (
        "Carregamento concluído para a tabela `{}`".format(nome_esperado)
        in logs
    )
    assert "adicionadas {} novas linhas.".format(linhas_esperadas) in logs


@pytest.mark.integracao
@pytest.mark.parametrize(
    "tabela_destino,variaveis",
    [
        (
            "dados_publicos.sisab_producao_municipios_por_categoria"
            + "_profissional_por_tipo_e",
            ("Tipo de Equipe", "Categoria Profissional"),
        ),
    ],
)
@pytest.mark.parametrize(
    "ano,mes,data_inicio,data_fim",
    [(2021, 9, None, None)],
)
@pytest.mark.parametrize(
    "unidade_geografica_tipo,unidades_geograficas_ids",
    [
        ("Municípios", ["e8cb5dcc-46d4-45af-a237-4ab683b8ce8e"]),
    ],
)
def teste_obter_relatorio_producao(
    sessao,
    tabela_destino,
    variaveis,
    ano,
    mes,
    data_inicio,
    data_fim,
    unidades_geograficas_ids,
    unidade_geografica_tipo,
    capfd,
):
    """Testa fazer ETL de dados de produção do SISAB para o BD da Impulso."""
    obter_relatorio_producao(
        sessao=sessao,
        tabela_destino=tabela_destino,
        variaveis=variaveis,
        ano=ano,
        mes=mes,
        data_inicio=data_inicio,
        data_fim=data_fim,
        unidades_geograficas_ids=unidades_geograficas_ids,
        unidade_geografica_tipo=unidade_geografica_tipo,
        teste=True,
    )

    logs = capfd.readouterr().err
    assert "Carregamento concluído para a tabela " in logs
    linhas_adicionadas = re.search("adicionadas ([0-9]+) novas linhas.", logs)
    assert linhas_adicionadas
    num_linhas_adicionadas = sum(
        int(num) for num in linhas_adicionadas.groups()
    )
    assert num_linhas_adicionadas > 0
