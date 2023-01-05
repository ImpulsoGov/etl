#from impulsoetl import __VERSION__
from impulsoetl.bd import tabelas, Sessao

from impulsoetl.cnes.estabelecimentos_identificados.principal import obter_informacoes_estabelecimentos_identificados
from impulsoetl.loggers import logger

agendamentos = tabelas["configuracoes.capturas_agendamentos"]
capturas_historico = tabelas["configuracoes.capturas_historico"]


with Sessao() as sessao:

    def capturar_agendamento(operacao_id, sessao = sessao):
        agendamentos_cnes = (
            sessao.query(agendamentos)
            .filte((agendamentos.c.operacao_id == operacao_id)
            .all()
            )
        )
    
    def cnes_estabelecimentos_identificados(
        teste: bool = False,
    )-> None:

        operacao_id  = "063b5cf8-34d1-744d-8f96-353d4f199171"

        agendamentos_cnes = capturar_agendamento(operacao_id,sessao=sessao)
        
        for agendamento in agendamentos_cnes:
            periodo_id = agendamentos_cnes.periodo_id
            unidade_geografica_id = agendamentos_cnes.unidade_geografica_id
            tabela_destino = agendamentos_cnes.tabela_destino
            codigo_sus_municipio = agendamentos_cnes.unidade_geografica_id_sus

            df_extraido = obter_informacoes_estabelecimentos_identificados(
                sessao=sessao,
                tabela_destino=tabela_destino,
                codigo_municipio=codigo_sus_municipio
            )
            df_extraido['periodo_id']=periodo_id
            df_extraido['unidade_geografica_id']=unidade_geografica_id

            if teste: 
                sessao.rollback()
                break

            logger.info("Registrando captura bem-sucedida...")

            requisicao_inserir_historico = capturas_historico.insert(
                {
                    "operacao_id": operacao_id,
                    "periodo_id": agendamento.periodo_id,
                    "unidade_geografica_id": agendamento.unidade_geografica_id,
                }
            )
            conector = sessao.connection()
            conector.execute(requisicao_inserir_historico)
            sessao.commit()
            logger.info("OK.")
