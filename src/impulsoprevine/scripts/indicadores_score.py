# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


import math
import os
import unicodedata
from datetime import datetime

import auxiliares.bancodedados as bancodedados
import auxiliares.utilitario as utilitario
import numpy as np
import pandas as pd
import unidecode
from uuid6 import uuid7


def diff_numerador_para_meta(row):
    if row.indicadores_resultados_porcentagem>row.indicadores_parametros_meta:
        return 0.0
    else:
        return (row.indicadores_parametros_meta/100*row.indicadores_resultados_denominador_utilizado)-row.indicadores_resultados_numerador

def diff_resultado_para_meta(row):
    if row.indicadores_resultados_porcentagem>row.indicadores_parametros_meta:
        return 0.0
    else:
        return round(row.indicadores_parametros_meta-row.indicadores_resultados_porcentagem, 1)

def identificacao_publico_alvo(row):
    diff = row.indicadores_resultados_denominador_indicado-row.indicadores_resultados_denominador_estimado
    if diff>=0:
        return 1.0
    else:
        return abs(diff)/100

def var_classificacao(row):
    value = row.var_quantidade_usuarios_total_meta*row.var_indicadores_score_premissas_acoes
    if value !=0:
        return value
    else:
        return 1.0

def classificacao(row):
    if row.indicadores_resultados_porcentagem<row.indicadores_parametros_meta:
        value = (
            1000*((
                1000*row.var_indicadores_parametros_peso*row.var_indicadores_score_premissas_validez
                )/row.var_classificacao)
        )/row.var_identificacao_publico_alvo
        return value
    else:
        return row.indicadores_parametros_meta-row.indicadores_resultados_porcentagem

def nivel(row, niveis):
    for i in niveis.index:
        if row.indicadores_nota_calculado==10.0:
            return 'Robusto'
        if niveis.nota_inicio[i]<=row.indicadores_nota_calculado<niveis.nota_fim[i]:
            return niveis.nivel[i]

def nivel_id(row, niveis):
    return niveis[niveis.nivel == row.indicadores_niveis_nivel].id.to_list()[0]

def decoding(row):
    return unidecode.unidecode(row.municipio_nome.replace("'", ""))

def tranforma(periodos):
    indicadores_score = pd.DataFrame(columns=['id', 'municipio_nome', 'estado_id', 'periodo_id', 'periodo_codigo', 
        'indicadores_parametros_id', 'indicadores_parametros_nome', 'indicadores_parametros_meta', 
        'indicadores_parametros_ordem', 'var_indicadores_parametros_peso', 'indicadores_resultados_id', 
        'indicadores_resultados_numerador', 'indicadores_resultados_denominador_estimado', 
        'indicadores_resultados_denominador_indicado', 'indicadores_resultados_denominador_utilizado', 
        'indicadores_resultados_porcentagem', 'indicadores_nota_calculado', 'indicadores_niveis_id', 
        'indicadores_niveis_nivel', 'indicadores_score_premissas_id', 'var_indicadores_score_premissas_acoes', 
        'var_indicadores_score_premissas_validez', 'indicadores_recomendacoes_id', 
        'indicadores_recomendacoes_recomendacao', 'diff_numerador_para_meta', 'diff_resultado_para_meta', 
        'var_quantidade_usuarios_total_meta', 'var_identificacao_publico_alvo', 'score', 'criacao_data', 
        'atualizacao_data', 'municipio_id_sus', 'var_classificacao', 'classificacao'])

    query = """
        SELECT r.municipio_id_sus, pop.municipio_nome, pop.estado_id, r.periodo_id, periodos.codigo as periodo_codigo, 
        r.indicadores_parametros_id, p.nome as indicadores_parametros_nome, p.meta as indicadores_parametros_meta, 
        p.ordem as indicadores_parametros_ordem, p.peso as var_indicadores_parametros_peso, 
        r.id as indicadores_resultados_id, r.numerador as indicadores_resultados_numerador, 
        r.denominador_estimado as indicadores_resultados_denominador_estimado, 
        r.denominador_informado as indicadores_resultados_denominador_indicado, 
        r.denominador_utilizado_calculado as indicadores_resultados_denominador_utilizado, 
        r.resultado_porcentagem as indicadores_resultados_porcentagem, 
        r.resultado_porcentagem_calculado as indicadores_resultados_porcentagem_calculado, 
        r.nota_calculado as indicadores_nota_calculado,
        ips.id as indicadores_score_premissas_id, 
        ips.acoes_por_usuario as var_indicadores_score_premissas_acoes,
        ips.validade_resultado as var_indicadores_score_premissas_validez, 
        ipr.id as indicadores_recomendacoes_id,
        ipr.recomendacao as indicadores_recomendacoes_recomendacao
        FROM previnebrasil.indicadores_resultados r
        JOIN previnebrasil.indicadores_parametros p ON p.id = r.indicadores_parametros_id
        JOIN dadospublicos.periodos periodos ON (periodos.id = r.periodo_id)
        JOIN dadospublicos.populacao pop ON (r.municipio_id_sus = pop.municipio_id_sus)
        JOIN previnebrasil.indicadores_premissas_score ips on ips.indicadores_parametros_id = r.indicadores_parametros_id
        JOIN previnebrasil.indicadores_premissas_recomendacoes ipr on ipr.indicadores_parametros_id = r.indicadores_parametros_id
        WHERE r.periodo_id = '{}'
        and ips.versao = (SELECT MAX(versao) FROM previnebrasil.indicadores_premissas_score)
        and ipr.versao = (SELECT MAX(versao) FROM previnebrasil.indicadores_premissas_recomendacoes);
        """.format(periodos.id[0])
    df = bancodedados.readQuery(query,'impulsogov-analitico')
    query = """
        SELECT id, nivel, nota_inicio, nota_fim
        FROM previnebrasil.indicadores_premissas_niveis
        WHERE versao = (SELECT MAX(versao) FROM previnebrasil.indicadores_premissas_niveis);
        """
    niveis = bancodedados.readQuery(query,'impulsogov-analitico')
    
    indicadores_score = indicadores_score.append(df)
    indicadores_score.municipio_nome = indicadores_score.apply(lambda x: decoding(x), axis=1)    
    indicadores_score.indicadores_niveis_nivel = indicadores_score.apply(lambda x: nivel(x, niveis), axis=1)
    indicadores_score.indicadores_niveis_id = indicadores_score.apply(lambda x: nivel_id(x, niveis), axis=1)
    indicadores_score.diff_numerador_para_meta = indicadores_score.apply(lambda x: diff_numerador_para_meta(x), axis=1)
    indicadores_score.diff_resultado_para_meta = indicadores_score.apply(lambda x: diff_resultado_para_meta(x), axis=1)
    indicadores_score.var_quantidade_usuarios_total_meta = indicadores_score.indicadores_parametros_meta/100*indicadores_score.indicadores_resultados_denominador_utilizado
    indicadores_score.var_identificacao_publico_alvo = indicadores_score.apply(lambda x: identificacao_publico_alvo(x), axis=1)
    indicadores_score.var_classificacao = indicadores_score.apply(lambda x: var_classificacao(x), axis=1)
    indicadores_score.classificacao = indicadores_score.apply(lambda x: classificacao(x), axis=1)
    indicadores_score.score = indicadores_score.groupby("municipio_id_sus")["classificacao"].rank("dense", ascending=False)
    indicadores_score.id = indicadores_score.apply(lambda row:uuid7(), axis=1)
    indicadores_score.criacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    indicadores_score.atualizacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return indicadores_score

def testes(resultados, periodos_antigos_lista):
    TESTS = {
        "DF não é um pd.DataFrame": lambda df: isinstance(resultados, pd.DataFrame),
        "Menos que 5570 cidades": lambda df: len(resultados["municipio_id_sus"].unique()) <= 5570,
        "Mais que 5500 cidades": lambda df: len(resultados["municipio_id_sus"].unique()) > 5500,
        "Período Novo": lambda df: resultados["periodo_id"].unique() not in periodos_antigos_lista
    }
    results = [v(resultados) for k, v in TESTS.items()]
    if not all(results):
        return False
    return True

def insere_atualiza(resultados, periodos):
    resultados = resultados[['id', 'municipio_nome', 'estado_id', 'periodo_id', 'periodo_codigo', 
        'indicadores_parametros_id', 'indicadores_parametros_nome', 'indicadores_parametros_meta', 
        'indicadores_parametros_ordem', 'var_indicadores_parametros_peso', 'indicadores_resultados_id', 
        'indicadores_resultados_numerador', 'indicadores_resultados_denominador_estimado', 
        'indicadores_resultados_denominador_indicado', 'indicadores_resultados_denominador_utilizado', 
        'indicadores_resultados_porcentagem', 'indicadores_nota_calculado', 'indicadores_niveis_id', 
        'indicadores_niveis_nivel', 'indicadores_score_premissas_id', 'var_indicadores_score_premissas_acoes', 
        'var_indicadores_score_premissas_validez', 'indicadores_recomendacoes_id', 
        'indicadores_recomendacoes_recomendacao', 'diff_numerador_para_meta', 'diff_resultado_para_meta', 
        'var_quantidade_usuarios_total_meta', 'var_identificacao_publico_alvo', 'score', 'criacao_data', 
        'atualizacao_data', 'municipio_id_sus']]
    tuple_list = [tuple(x) for x in resultados.to_records(index=False)]
    chunks = [tuple_list[x:x+800000] for x in range(0, len(tuple_list), 800000)]
    for chunk in chunks:
        query = """
        INSERT INTO previnebrasil.indicadores_score
        (id, municipio_nome, estado_id, periodo_id, periodo_codigo, indicadores_parametros_id, indicadores_parametros_nome, indicadores_parametros_meta, indicadores_parametros_ordem, var_indicadores_parametros_peso, indicadores_resultados_id, indicadores_resultados_numerador, indicadores_resultados_denominador_estimado, indicadores_resultados_denominador_indicado, indicadores_resultados_denominador_utilizado, indicadores_resultados_porcentagem, indicadores_nota_calculado, indicadores_niveis_id, indicadores_niveis_nivel, indicadores_score_premissas_id, var_indicadores_score_premissas_acoes, var_indicadores_score_premissas_validez, indicadores_recomendacoes_id, indicadores_recomendacoes_recomendacao, diff_numerador_para_meta, diff_resultado_para_meta, var_quantidade_usuarios_total_meta, var_identificacao_publico_alvo, score, criacao_data, atualizacao_data, municipio_id_sus)
        VALUES{};
        """.format(str(chunk).replace('%', '%%').replace("[","").replace("]","").replace(",u'", ",'").replace(", u'", ",'").replace("1L", "1").replace("0L", "0").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace(", nan)", ", NULL)").replace("None,", "NULL,").replace("nan,", "NULL,").replace("(u'","('"))
        bancodedados.executeQuery(query,'impulsogov-analitico')
    utilitario.update_quadrimestre('indicadores_score', periodos.data[0], 'impulsogov-analitico')


def main():
    query_periodos = """
        SELECT tabela, "data", periodo_codigo, id
        FROM previnebrasil.parametro_datacaptura
        JOIN dadospublicos.periodos ON periodo_codigo = codigo
        WHERE tabela = 'indicadores_score';
        """
    periodos = bancodedados.readQuery(query_periodos,'impulsogov-analitico')
    query_periodos_antigos = """
        SELECT DISTINCT periodo_id
        FROM previnebrasil.indicadores_score;
        """
    periodos_antigos = bancodedados.readQuery(query_periodos_antigos,'impulsogov-analitico')
    resultados = tranforma(periodos)
    if testes(resultados, periodos_antigos.periodo_id.to_list()):
        insere_atualiza(resultados,periodos)
    return True