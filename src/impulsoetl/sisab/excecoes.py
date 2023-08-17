# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Exceções relacionadas à interação com a interface do SISAB."""

from sqlalchemy.orm import Session
from sqlalchemy import insert
from impulsoetl.bd import tabelas,Sessao



class SisabExcecao(Exception):
    """Exceção base para os erros do SISAB."""
    def __init__(self,exception):
        self.exception = str(exception)

    def insere_erro_database(self,sessao,traceback_str,operacao_id,periodo_id):

        tabela_destino = tabelas["configuracoes.capturas_erros_etl"]
        requisicao_inserir_historico = insert(tabela_destino).values(operacao_id=operacao_id,periodo_id=periodo_id,unidade_geografica_id='28de805e-5bdc-49c3-863c-2cf87f95e371',erro_mensagem=self.exception,erro_traceback=str(traceback_str))
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()


class SisabErroCompetenciaInexistente:
    """A competência indicada não está disponível na interface do SISAB."""
    def __init__(self,exception):
        self.exception = exception 

    def insere_erro_database(self,sessao,traceback_str,operacao_id,periodo_id,unidade_geografica_id):

        if unidade_geografica_id is None:
            unidade_geografica_id = '28de805e-5bdc-49c3-863c-2cf87f95e371'
        else:
            unidade_geografica_id = unidade_geografica_id

        tabela_destino = tabelas["configuracoes.capturas_erros_etl"]
        requisicao_inserir_historico = insert(tabela_destino).values(operacao_id=operacao_id,periodo_id=periodo_id,unidade_geografica_id=unidade_geografica_id,erro_mensagem=self.exception,erro_traceback=str(traceback_str))
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()



class SisabErroPreenchimentoIncorreto(SisabExcecao, ValueError):
    """O formulário indicou violação de alguma regra de preenchimento."""

    pass


class SisabErroRotuloOuValorInexistente(SisabExcecao, ValueError):
    """Algum parâmetro indicado não está disponível na interface do SISAB."""

    pass
