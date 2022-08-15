# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


from datetime import datetime

import bancodedados
import pandas as pd
from dateutil.relativedelta import relativedelta
from uuid6 import uuid7

resultados = pd.DataFrame(columns=['id', 'data_inicio', 'data_fim', 'codigo', 'tipo'])
data_inicio = datetime.strptime('1990-01-01', '%Y-%m-%d')
for i in range(183):
    data_fim = data_inicio + relativedelta(months=+4)
    fim = data_fim-relativedelta(days=1)
    resultados = resultados.append({'data_inicio':data_inicio.strftime('%Y-%m-%d'),'data_fim':fim.strftime('%Y-%m-%d'),'codigo':str(fim.year) + '.Q' + str(int(fim.month/4))}, ignore_index=True)
    data_inicio = data_fim
resultados.id = resultados.apply(lambda row : uuid7(), axis=1)
resultados.tipo = 'Quadrimestral'
tuple_list = [tuple(x) for x in resultados.to_records(index=False)]
chunks = [tuple_list[x:x+800000] for x in range(0, len(tuple_list), 800000)]
for chunk in chunks:
    query = """
    INSERT INTO dadospublicos.periodos
    (id, data_inicio, data_fim, codigo, tipo)
    VALUES{};
    """.format(str(chunk).replace('%', '%%').replace("[","").replace("]","").replace(",u'", ",'").replace(", u'", ",'").replace("1L", "1").replace("0L", "0").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace(", nan)", ", NULL)").replace("None,", "NULL,").replace("nan,", "NULL,").replace("(u'","('"))
    bancodedados.executeQuery(query,'local')


resultados = pd.DataFrame(columns=['id', 'data_inicio', 'data_fim', 'codigo', 'tipo'])
data_inicio = datetime.strptime('1990-01-01', '%Y-%m-%d')
for i in range(732):
    data_fim = data_inicio + relativedelta(months=+1)
    fim = data_fim-relativedelta(days=1)
    resultados = resultados.append({'data_inicio':data_inicio.strftime('%Y-%m-%d'),'data_fim':fim.strftime('%Y-%m-%d'),'codigo':str(fim.year) + '.M' + str(int(fim.month))}, ignore_index=True)
    data_inicio = data_fim
resultados.id = resultados.apply(lambda row : uuid7(), axis=1)
resultados.tipo = 'Mensal'
tuple_list = [tuple(x) for x in resultados.to_records(index=False)]
chunks = [tuple_list[x:x+800000] for x in range(0, len(tuple_list), 800000)]
for chunk in chunks:
    query = """
    INSERT INTO dadospublicos.periodos
    (id, data_inicio, data_fim, codigo, tipo)
    VALUES{};
    """.format(str(chunk).replace('%', '%%').replace("[","").replace("]","").replace(",u'", ",'").replace(", u'", ",'").replace("1L", "1").replace("0L", "0").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace(", nan)", ", NULL)").replace("None,", "NULL,").replace("nan,", "NULL,").replace("(u'","('"))
    bancodedados.executeQuery(query,'local')


resultados = pd.DataFrame(columns=['id', 'data_inicio', 'data_fim', 'codigo', 'tipo'])
data_inicio = datetime.strptime('1990-01-01', '%Y-%m-%d')
for i in range(244):
    data_fim = data_inicio + relativedelta(months=+3)
    fim = data_fim-relativedelta(days=1)
    resultados = resultados.append({'data_inicio':data_inicio.strftime('%Y-%m-%d'),'data_fim':fim.strftime('%Y-%m-%d'),'codigo':str(fim.year) + '.T' + str(int(fim.month/3))}, ignore_index=True)
    data_inicio = data_fim
resultados.id = resultados.apply(lambda row : uuid7(), axis=1)
resultados.tipo = 'Trimestral'
tuple_list = [tuple(x) for x in resultados.to_records(index=False)]
chunks = [tuple_list[x:x+800000] for x in range(0, len(tuple_list), 800000)]
for chunk in chunks:
    query = """
    INSERT INTO dadospublicos.periodos
    (id, data_inicio, data_fim, codigo, tipo)
    VALUES{};
    """.format(str(chunk).replace('%', '%%').replace("[","").replace("]","").replace(",u'", ",'").replace(", u'", ",'").replace("1L", "1").replace("0L", "0").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace(", nan)", ", NULL)").replace("None,", "NULL,").replace("nan,", "NULL,").replace("(u'","('"))
    bancodedados.executeQuery(query,'local')


resultados = pd.DataFrame(columns=['id', 'data_inicio', 'data_fim', 'codigo', 'tipo'])
data_inicio = datetime.strptime('1990-01-01', '%Y-%m-%d')
for i in range(122):
    data_fim = data_inicio + relativedelta(months=+6)
    fim = data_fim-relativedelta(days=1)
    resultados = resultados.append({'data_inicio':data_inicio.strftime('%Y-%m-%d'),'data_fim':fim.strftime('%Y-%m-%d'),'codigo':str(fim.year) + '.S' + str(int(fim.month/6))}, ignore_index=True)
    data_inicio = data_fim
resultados.id = resultados.apply(lambda row : uuid7(), axis=1)
resultados.tipo = 'Semestral'
tuple_list = [tuple(x) for x in resultados.to_records(index=False)]
chunks = [tuple_list[x:x+800000] for x in range(0, len(tuple_list), 800000)]
for chunk in chunks:
    query = """
    INSERT INTO dadospublicos.periodos
    (id, data_inicio, data_fim, codigo, tipo)
    VALUES{};
    """.format(str(chunk).replace('%', '%%').replace("[","").replace("]","").replace(",u'", ",'").replace(", u'", ",'").replace("1L", "1").replace("0L", "0").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace(", nan)", ", NULL)").replace("None,", "NULL,").replace("nan,", "NULL,").replace("(u'","('"))
    bancodedados.executeQuery(query,'local')