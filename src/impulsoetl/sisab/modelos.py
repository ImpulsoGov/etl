# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Declara representações das tabelas relativas ao SIASUS."""


from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declared_attr

# from sqlalchemy_continuum import make_versioned


# TODO: habilitar versionamento
# make_versioned(options={"native_versioning": True})


class TabelaProducao(object):
    """Modelo genérico para tabelas de produção do SISAB."""

    _definicoes = {
        "id": "Identificador do agregado de produção no banco de dados",
        "periodo_id": "Identificador do período de realização da produção.",
        "unidade_geografica_id": "Identificador da unidade geográfica "
        + "responsável pela produção.",
        "tipo_producao": "Tipo de contato assistencial registrado. Pode ser "
        + "um entre 'Atendimento Individual', 'Atendimento Odontológico', "
        + "'Procedimento' ou 'Visita Domiciliar'.",
        "quantidade_registrada": "Número de procedimentos ou contatos "
        + "assistenciais informados pelo município no período.",
    }

    # __versioned__ = versionamento_parametros

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower().split(".")[-1]

    @declared_attr
    def id(cls):
        return sa.Column(
            "id",
            UUID(as_uuid=False),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
            unique=True,
            index=True,
            comment=cls._definicoes["id"],
            doc=cls._definicoes["id"],
        )

    @declared_attr
    def periodo_id(cls):
        return sa.Column(
            "periodo_id",
            UUID(as_uuid=False),
            # BUG: sqlalchemy.exc.NoReferencedTableError: Foreign key
            # associated with column 'dados_publicos.sisab_producao_municipios
            # _por_conduta_por_problema_condicao_avaliada.periodo_id' could not
            # find table 'listas_de_codigos.periodos' with which to generate a
            # foreign key to target column 'id'
            # Associado à falta do relacionamento com a tabela de período; ver
            # ao final desta classe.
            # sa.ForeignKey("listas_de_codigos.periodos.id"),
            nullable=False,
            index=True,
            comment=cls._definicoes["periodo_id"],
            doc=cls._definicoes["periodo_id"],
        )

    @declared_attr
    def unidade_geografica_id(cls):
        return sa.Column(
            "unidade_geografica_id",
            UUID(as_uuid=False),
            # BUG: sqlalchemy.exc.NoReferencedTableError: Foreign key
            # Associado à falta do relacionamento com a tabela de período; ver
            # ao final desta classe.
            # sa.ForeignKey("listas_de_codigos.municipios.id"),
            nullable=False,
            index=True,
            comment=cls._definicoes["unidade_geografica_id"],
            doc=cls._definicoes["unidade_geografica_id"],
        )

    @declared_attr
    def tipo_producao(cls):
        return sa.Column(
            "tipo_producao",
            sa.Text,
            nullable=False,
            comment=cls._definicoes["tipo_producao"],
            doc=cls._definicoes["tipo_producao"],
        )

    @declared_attr
    def quantidade_registrada(cls):
        return sa.Column(
            "quantidade_registrada",
            sa.SMALLINT,
            nullable=False,
            comment=cls._definicoes["quantidade_registrada"],
            doc=cls._definicoes["quantidade_registrada"],
        )

    # -------------------------------------------------------------------------

    # TODO: Adicionar relacionamentos com tabelas de períodos e municípios.

    # BUG: sqlalchemy.exc.InvalidRequestError: When initializing mapper mapped
    # class dados_publicos.sisab_producao_municipios_por_categoria_profissional
    # _por_tipo_equipe->dados_publicos.sisab_producao_municipios_por_categoria
    # _profissional_por_tipo_equipe, expression 'listas_de_codigos.periodos'
    # failed to locate a name ('listas_de_codigos.periodos'). If this is a
    # class name, consider adding this relationship() to the <class
    # 'impulsoetl.sisab.producao.dados_publicos.
    # sisab_producao_municipios_por_categoria_profissional_por_tipo_equipe'>
    # class after both dependent classes have been defined.

    # -------------------------------------------------------------------------

    # @declared_attr
    # def periodo(cls):
    #     return sa.orm.relationship("listas_de_codigos.periodos")

    # @declared_attr
    # def municipio(cls):
    #     return sa.orm.relationship("listas_de_codigos.municipios")
