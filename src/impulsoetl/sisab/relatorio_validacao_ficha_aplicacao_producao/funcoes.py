# flake8: noqa
# type: ignore
import requests
from requests.models import Response
from io import StringIO
import pandas as pd
import json
from datetime import datetime
import uuid

from sqlalchemy.orm import Session
from sqlalchemy import delete

from impulsoetl.loggers import logger
from impulsoetl.bd import tabelas
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.sisab.relatorio_validacao_ficha_aplicacao_producao.suporte_extracao import head


def obter_lista_registros_inseridos(
    sessao: Session,
    tabela_alvo: str,
    ficha_tipo: str,
    aplicacao_tipo: str
    ):
    """Obtém lista de registro da períodos que já constam na tabela

        Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite acessar a base de dados da ImpulsoGov.
        tabela_alvo: Tabela alvo da busca.
        ficha_tipo: Tipo de ficha que será filtro para a requisição no sisab
        aplicacao_tipo: Tipo de aplicacao que será filtro para a requisicão no sisab


    Retorna:
        Lista de períodos que já constam na tabela destino filtrados por ficha e aplicação.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """  
    

    tabela = tabelas[tabela_alvo]
    registros = sessao.query(tabela.c.periodo_codigo).distinct().where((tabela.c.periodo_codigo == periodo_codigo)\
            and((tabela.c.ficha == ficha_tipo)\
                and((tabela.c.aplicacao == aplicacao_tipo)))).all()
    sessao.commit()

    registros_codigos = [periodo.periodo_codigo for periodo in registros]
    logger.info("Leitura dos períodos inseridos no banco Impulso OK!")
    return registros_codigos

def competencia_para_periodo_codigo(periodo_competencia):
    """Essa função converte o período de competência de determinado relatorio no código do periodo padrão da impulso
    EX: 202203 para 2022.M3
    Args:
        periodo_competencia (str): período de competência de determinado relatório
    Returns:
        periodo código
    """


    ano = periodo_competencia[0:4]
    mes = ".M" + periodo_competencia[4:6]
    if mes[2] == '0':
        mes = ".M" + periodo_competencia[5:6]
        periodo_codigo = ano + mes
    else:
        periodo_codigo = ano + mes
    return periodo_codigo


def obter_data_criacao(
    sessao: Session,
    tabela_destino: str,
    periodo_codigo: str,
    ) -> datetime:
    """Obtém a data de criação do registro a partir do código do período.
    Argumentos:
        tabela_alvo: Tabela alvo da busca.
        periodo_codigo: Período de referência da data.
    Retorna:
        Data de criação do registro, como um objeto `datetime`.
    """

    tabela = tabelas[tabela_destino]

    data_criacao_obj = (
    sessao.query(tabela)
    .filter(tabela.c.periodo_codigo == periodo_codigo)
    .first()
    )
    sessao.commit()

    try:
        return data_criacao_obj.criacao_data  # type: ignore
    except AttributeError:
        return datetime.now()


def requisicao_validacao_sisab_producao_ficha_aplicacao(periodo_competencia,ficha_codigo,aplicacao_codigo,envio_prazo):
    """Obtém os dados da API
    
    Args:
        periodo_competencia: Período de competência do dado a ser buscado no sisab
        envio_prazo(bool): Tipo de relatório de validação a ser obtido (referência check box "no prazo" no sisab)  
    
    Returns:
    resposta: Resposta da requisição do sisab, com os dados obtidos ou não
    """

    print(f'{periodo_competencia},{ficha_codigo},{aplicacao_codigo},{envio_prazo}')
    url = "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/envio/RelValidacao.xhtml"
    periodo_tipo='producao'
    hd = head(url)
    vs = hd[1] #viewstate
    payload='j_idt44=j_idt44&unidGeo=brasil&periodo='+periodo_tipo+'&j_idt70='+periodo_competencia+'&colunas=regiao&colunas=uf&colunas=ibge&colunas=municipio&colunas=cnes&colunas=tp_unidade&colunas=ine&colunas=tp_equipe'+ficha_codigo+aplicacao_codigo+envio_prazo+'&javax.faces.ViewState='+vs+'&j_idt102=j_idt102'
    headers = hd[0]
    resposta = requests.request("POST", url, headers=headers, data=payload)
    logger.info("Dados Obtidos no SISAB")
    return resposta


def tratamento_validacao_producao_ficha_aplicacao(sessao,resposta,data_criacao,ficha_tipo,aplicacao_tipo,envio_prazo,periodo_codigo):
    """Tratamento dos dados obtidos 

    Args:
    resposta (requests.models.Response): Resposta da requisição efetuada no sisab


    Returns:
    df: dataframe com os dados enriquecidos e tratados em formato pandas dataframe  
    """


    logger.info("Dados em tratamento")

    envio_prazo_on = '&envioPrazo=on'
    
    df_obtido = pd.read_csv(StringIO(resposta.text),sep=';',encoding = 'ISO-8859-1', skiprows=range(0,4), skipfooter=4,  engine='python') #ORIGINAL DIRETO DA EXTRAÇÃO

    assert df_obtido['Uf'].count() > 26, "Estado faltante"
    
    df_obtido[['INE','Tipo Unidade','Tipo Equipe']] = df_obtido[['INE','Tipo Unidade','Tipo Equipe']].fillna('0').astype('int')

    colunas = ["id",
        "municipio_id_sus",
        "periodo_id",
        "cnes_id",
        "cnes_nome",
        "ine_id",
        "ine_tipo",
        "ficha",
        "aplicacao",           
        "validacao_nome",
        "validacao_quantidade",
        "criacao_data",
        "atualizacao_data",
        "periodo_codigo",
        "no_prazo",
        "municipio_nome",
        "unidade_geografica_id"]

    df = pd.DataFrame(columns=colunas)      

    periodo_id = (
        sessao.query(tabela_periodos)  # type: ignore
        .filter(tabela_periodos.c.codigo == periodo_codigo)
        .first()
        .id
    )
    sessao.commit()

    df["municipio_id_sus"] = df_obtido['IBGE']

    df["periodo_id"] = periodo_id

    df["cnes_id"] = df_obtido["CNES"]

    df["cnes_nome"]= df_obtido["Tipo Unidade"]

    df["ine_id"] = df_obtido["INE"]

    df["ine_tipo"] = df_obtido["Tipo Equipe"]
    
    df["validacao_nome"] = df_obtido["Validação"]

    df["validacao_quantidade"] = df_obtido["Total"]

    df["municipio_nome"] = df_obtido["Municipio"]

    df['id'] = df.apply(lambda row:uuid.uuid4(), axis=1)

    df["atualizacao_data"] = pd.Timestamp.now()

    df["criacao_data"] = data_criacao

    df["periodo_codigo"] = periodo_codigo

    df["no_prazo"] = 1 if (envio_prazo == envio_prazo_on) else 0 

    df["ficha"] = ficha_tipo

    df["aplicacao"] = aplicacao_tipo

    df["unidade_geografica_id"] = df["municipio_id_sus"].apply(lambda row: id_sus_para_id_impulso(sessao, id_sus=row))

    df[["id","municipio_id_sus", "periodo_id","cnes_id","cnes_nome",\
        "ine_id","ine_tipo","ficha","aplicacao","validacao_nome","periodo_codigo","municipio_nome"]] = df[["id","municipio_id_sus", "periodo_id","cnes_id","cnes_nome",\
        "ine_id","ine_tipo","ficha","aplicacao","validacao_nome","periodo_codigo","municipio_nome"]].astype("string")
    
    df["validacao_quantidade"] = df["validacao_quantidade"].astype("int")
    df["no_prazo"] = df["no_prazo"].astype("bool")
    
    df['atualizacao_data'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df["criacao_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # temporário somente teste

    df_validacao_tratado = df 

    logger.info("Dados tratados")

    print(df_validacao_tratado.head())
    
    return df_validacao_tratado


def testes_pre_carga_validacao_ficha_aplicacao_producao (df_validacao_tratado):
    """Realiza algumas validações no dataframe antes da carga ao banco.
    Argumentos:
            relatorio_validacao_df: [`DataFrame`][] contendo os dados a serem carregados
            na tabela de destino, já no formato utilizado pelo banco de dados
            da ImpulsoGov.
    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.
    """


    assert [
            (sum(df_validacao_tratado['cnes_id'].isna()) == 0, "Dado ausente em cnes_id"),
            (sum(df_validacao_tratado['id'].isna()) == 0, "Id do registro ausente"),
            (sum(df_validacao_tratado['ine_id'].isna()) == 0, "ine_id ausente"),
            (sum(df_validacao_tratado['ficha'].isna()) == 0, "Nome da ficha ausente"),
            (sum(df_validacao_tratado['aplicacao'].isna()) == 0, "Nome da aplicacao ausente"),
            (sum(df_validacao_tratado['validacao_nome'].isna()) == 0, "Nome da validacão ausente"),
            (sum(df_validacao_tratado['municipio_nome'].isna()) == 0, "Nome de município ausente"),
            (df_validacao_tratado['unidade_geografica_id'].nunique() == df_validacao_tratado['municipio_id_sus'].nunique() , "Falta de unidade geográfica"),
            (sum(df_validacao_tratado['validacao_quantidade']) > 0, "Quantidade de validação inválida"),
            (len(df_validacao_tratado.columns) == 17, "Falta de coluna no dataframe")
    ]

    logger.info("Testes OK!")


def carregar_validacao_ficha_aplicacao_producao(sessao,df_validacao_tratado,periodo_competencia,ficha_tipo,aplicacao_tipo,tabela_destino):
    """Carrega os dados de um arquivo validação do portal SISAB no BD da Impulso.
    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        relatorio_validacao_df: [`DataFrame`][] contendo os dados a serem carregados
            na tabela de destino, já no formato utilizado pelo banco de dados
            da ImpulsoGov.
    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.
     """

        
    relatorio_validacao_df = df_validacao_tratado

    registros = json.loads(
        relatorio_validacao_df.to_json(
            orient="records",
            date_format="iso",
        )
    )


    tabela_relatorio_validacao = tabelas[tabela_destino]

    periodo_codigo = competencia_para_periodo_codigo(periodo_competencia)

    registros_inseridos = obter_lista_registros_inseridos(sessao,ficha_tipo,aplicacao_tipo)


    if periodo_codigo in registros_inseridos:

        limpar = delete(tabela_relatorio_validacao)\
        .where((tabela_relatorio_validacao.c.periodo_codigo == periodo_codigo)\
            and((tabela_relatorio_validacao.c.ficha == ficha_tipo)\
                and((tabela_relatorio_validacao.c.aplicacao == aplicacao_tipo))))
        logger.debug(limpar)
        sessao.execute(limpar)
        

    requisicao_insercao = tabela_relatorio_validacao.insert().values(registros)
    sessao.execute(requisicao_insercao)
    
    logger.info(
    "Carregamento concluído para a tabela `{tabela_nome}`: "
    + "adicionadas {linhas_adicionadas} novas linhas.",
    tabela_nome=tabelas[tabela_destino], 
    linhas_adicionadas=len(relatorio_validacao_df))

    return 0


def obter_validacao_ficha_aplicacao_producao(
    sessao: Session,
    periodo_competencia,
    ficha_tipo: str,
    aplicacao_tipo: str,
    ficha_codigo: str,
    aplicacao_codigo: str,
    envio_prazo: bool,
    tabela_destino: str,
    periodo_codigo: str,
) -> None:
    """Executa o ETL de relatórios de validação dos envios ao SISAB.
    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        periodo_competencia: Data de início do período de referência do dado.
        envio_prazo: Indica se os relatórios de validação a serem considerados
            apenas os enviados no prazo (`True`) ou se devem considerar tanto
            envios no prazo quanto fora do prazo (`False`).
        tabela_destino: Nome da tabela no banco de dados da ImpulsoGov para
            onde serão carregados os dados capturados (no formato
            `nome_do_schema.nome_da_tabela`).
        periodo_codigo: Código do período de referência.
    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """

    data_criacao = obter_data_criacao(sessao, tabela_destino, periodo_codigo)

    resposta = requisicao_validacao_sisab_producao_ficha_aplicacao(
        periodo_competencia,
        ficha_codigo,
        aplicacao_codigo,
        envio_prazo
    )

    df_validacao_tratado = tratamento_validacao_producao_ficha_aplicacao(
        sessao,
        resposta,
        data_criacao,
        ficha_tipo,
        aplicacao_tipo,
        envio_prazo,
        periodo_codigo)
        

    testes_pre_carga_validacao_ficha_aplicacao_producao(df_validacao_tratado)

    carregar_validacao_ficha_aplicacao_producao(
        sessao,
        df_validacao_tratado,
        periodo_competencia,
        ficha_tipo,
        aplicacao_tipo,
        tabela_destino
    )

    logger.info("Dados prontos para o commit")
    return None



