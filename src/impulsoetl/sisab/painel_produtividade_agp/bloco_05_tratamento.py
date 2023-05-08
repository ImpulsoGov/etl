import pandas as pd
import numpy as np
import glob
import os

# Lê todos os arquivos csv do diretório e concatena em df_consolidado
path = r'C:\Users\maira\Impulso\painel_de_produtividade\bloco_05_v2\extracoes_municipios' 
arquivos = glob.glob(os.path.join(path , "*.csv"))

colunas = ['municipio_uf','municipio_nome','municipio_id_sus','periodo_data','categoria_profissional','problema_condicao_avaliada','conduta','quantidade']
df_consolidado = pd.DataFrame(columns = colunas)

for nome_arquivo in arquivos:
    df = pd.read_csv(nome_arquivo)
    df = df.copy().reset_index(drop=True)
    df = df[['municipio_uf','municipio_nome','municipio_id_sus','periodo_data','categoria_profissional','problema_condicao_avaliada','conduta','quantidade']]
    df_consolidado = df_consolidado.append(df)

# Cria coluna com as porcentagens de encaminhamento de cada problema_condicao_avaliada
df_consolidado = df_consolidado.sort_values(by=['municipio_nome','periodo_data','categoria_profissional','problema_condicao_avaliada'])
df_consolidado['total_problema_condicao'] = df_consolidado.groupby(['problema_condicao_avaliada','municipio_id_sus','categoria_profissional','municipio_nome','periodo_data'])['quantidade'].transform(np.sum)
df_consolidado['porcentagem_encaminhamento'] = df_consolidado['quantidade']/df_consolidado['total_problema_condicao']

#Lista os estados e seus respectivos códigos e substitui os valores na coluna municipio_uf
estados = {
    26:"PE",
    35:"SP",
    42:"SC",
    31:"MG"
}
df_consolidado =  df_consolidado.replace({"municipio_uf":estados})

# Cria coluna quantidade_equipes
equipes = {
    'Apiúna':4,
    'Ibirama':9,
    'Igarassu':33,
    'Itapissuma':11,
    'Juquitiba':8,
    'Lontras':5,
    'São Gonçalo do Abaeté':4,
    'Tapiraí':2,
    'Três Marias':10,
    'Vitória de Santo Antão':37
}

df_consolidado['quantidade_equipes'] = df_consolidado['municipio_nome'].map(equipes)

# Cria coluna das condições previne
condicoes_previne = ['Hipertensao arterial','Diabetes','Pre-natal','Rastreamento de cancer do colo do utero','Puerperio (ate 42 dias)','Rastreamento de risco cardiovascular']
df_consolidado["PrevineBrasil"] = np.where(df_consolidado["problema_condicao_avaliada"].isin(condicoes_previne), "Sim", "Não")

df_consolidado = df_consolidado.reset_index(drop=True)
#df_consolidado.to_csv(r"C:\Users\maira\Impulso\painel_de_produtividade\bloco_05_v2\df_consolidado.csv", index=False, encoding='utf-8') 