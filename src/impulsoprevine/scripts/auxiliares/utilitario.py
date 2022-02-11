# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


from datetime import datetime
from dateutil.relativedelta import relativedelta
import auxiliares.bancodedados as bancodedados
import os



def update_quadrimestre(tabela, quadrimestre, banco):
    quadrimestre = datetime.strptime(quadrimestre, '%Y%m') + relativedelta(months=+4)
    quadrimestre_codigo = str(quadrimestre.year) + '.Q' + str(int(quadrimestre.month/4))
    quadrimestre = quadrimestre.strftime("%Y%m")
    query = """
    UPDATE previnebrasil.parametro_datacaptura
    SET "data"='{}', periodo_codigo='{}'
    WHERE tabela='{}';
    """.format(quadrimestre,quadrimestre_codigo, tabela)
    bancodedados.executeQuery(query, banco)
    return True

def update_mes(tabela, mes, banco):
    mes = datetime.strptime(mes, '%Y%m') + relativedelta(months=+1)
    mes_codigo = str(mes.year) + '.M' + str(mes.month)
    mes = mes.strftime("%Y%m")
    query = """
    UPDATE previnebrasil.parametro_datacaptura
    SET "data"='{}', periodo_codigo='{}'
    WHERE tabela='{}';
    """.format(mes,mes_codigo, tabela)
    bancodedados.executeQuery(query, banco)
    return True

def limpa_temporario(temppath):
    filelist = [f for f in os.listdir(temppath) ]
    for f in filelist:
            caminho = './temporario/{}'.format(f)
            os.remove(os.path.join(temppath, f))
    return True