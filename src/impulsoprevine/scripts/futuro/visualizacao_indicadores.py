import pandas as pd
import numpy as np
import os
import uuid
import auxiliares.bancodedados as bancodedados
import auxiliares.utilitario as utilitario
from datetime import datetime

def main():
    query = """
        SELECT tabela, "data", periodo_codigo, id
        FROM previnebrasil.parametro_datacaptura
        JOIN dadospublicos.periodos ON periodo_codigo = codigo
        WHERE tabela = 'visualizacao_indicadores';
        """
    periodos = bancodedados.readQuery(query,'impulsogov-analitico')

    visualizacao_indicadores = pd.DataFrame(columns=['id','municipio_id_sus', 'municipio_nome', 'estado_id',
        'periodo_id', 'periodo_codigo', 'indicadores_parametros_id', 'indicadores_parametros_nome', 
        'indicadores_parametros_peso', 'indicadores_parametros_meta', 'indicadores_parametros_ordem',
        'indicadores_score_id', 'indicadores_resultados_porcentagem', 'indicadores_nota_calculado',
        'score', 'recomendacao','indicadores_niveis_nivel', 'diff_numerador_para_meta',
        'criacao_data', 'atualizacao_data'])
    query = """
        SELECT municipio_id_sus, municipio_nome, estado_id,
        periodo_id, periodo_codigo,  
        indicadores_parametros_id, indicadores_parametros_nome, 
        var_indicadores_parametros_peso as indicadores_parametros_peso, 
        indicadores_parametros_meta, indicadores_parametros_ordem,
        id as indicadores_score_id, 
        indicadores_resultados_porcentagem, indicadores_nota_calculado,
        score, indicadores_recomendacoes_recomendacao as recomendacao,
        indicadores_niveis_nivel, diff_numerador_para_meta
        FROM previnebrasil.indicadores_score
        WHERE periodo_id = '{}';
        """.format(periodos.id[0])
    df = bancodedados.readQuery(query,'impulsogov-analitico')

    visualizacao_indicadores = visualizacao_indicadores.append(df)
    visualizacao_indicadores.id = visualizacao_indicadores.apply(lambda row:uuid.uuid4(), axis=1)
    visualizacao_indicadores.criacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    visualizacao_indicadores.atualizacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    visualizacao_indicadores = visualizacao_indicadores[['id', 'municipio_id_sus', 'municipio_nome', 
        'estado_id', 'periodo_id', 'periodo_codigo', 'indicadores_parametros_id', 
        'indicadores_parametros_nome', 'indicadores_parametros_meta', 'indicadores_parametros_ordem', 
        'indicadores_parametros_peso', 'indicadores_score_id', 'indicadores_resultados_porcentagem', 
        'indicadores_nota_calculado', 'indicadores_niveis_nivel', 'recomendacao', 'diff_numerador_para_meta', 
        'score', 'criacao_data', 'atualizacao_data']]

    tuple_list = [tuple(x) for x in visualizacao_indicadores.to_records(index=False)]
    chunks = [tuple_list[x:x+800000] for x in range(0, len(tuple_list), 800000)]

    for chunk in chunks:
        query = """
        INSERT INTO impulsoprevine.visualizacao_indicadores
        (id, municipio_id_sus, municipio_nome, estado_id, periodo_id, periodo_codigo, indicadores_parametros_id, 
        indicadores_parametros_nome, indicadores_parametros_meta, indicadores_parametros_ordem, 
        indicadores_parametros_peso, indicadores_score_id, indicadores_resultados_porcentagem, 
        indicadores_nota_calculado, indicadores_niveis_nivel, recomendacao, diff_numerador_para_meta, 
        score, criacao_data, atualizacao_data)
        VALUES{};
        """.format(str(chunk).replace('%', '%%').replace("[","").replace("]","").replace(",u'", ",'").replace(", u'", ",'").replace("1L", "1").replace("0L", "0").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace(", nan)", ", NULL)").replace("None,", "NULL,").replace("nan,", "NULL,").replace("(u'","('"))
        bancodedados.executeQuery(query,'impulso-producao')
    utilitario.update_quadrimestre('visualizacao_indicadores', periodos.data[0], 'impulsogov-analitico')
    return True