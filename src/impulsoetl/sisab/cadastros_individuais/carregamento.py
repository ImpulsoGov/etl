import bancodedados as bancodedados

def insereBanco(df):
    try:
        query = f"""
        INSERT INTO sisab_cadastros_municipios_teste
        (id, periodo_id, municipio_id_sus, periodo_id, periodo_codigo, cnes_id, cnes_nome, ine_id, equipes_validas, criterio_pontuacao, criacao_data, atualizacao_data)
        VALUES('{df.id}','{df.unidade_geografica_id_sus}','{df.municipio_id_sus}','{df.periodo_id}','{df.periodo_codigo}','{df.estabelecimento_id_cnes}','{df.estabelecimento_nome}','{df.equipe_id_ine}',{df.equipe_validas},{df.criterio_pontuacao_possui},'{df.criacao_data}','{df.atualizacao_data}');
        """
        qy = bancodedados.executeQuery(query,'impulsogov-analitico')
        print(qy)
    except Exception as e:
      print(e)
      print('erro') 