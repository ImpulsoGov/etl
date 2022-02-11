# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Tipos customizados para checagem estária."""


from datetime import date, datetime
from typing import Union

from pandas import Timestamp

DatetimeLike = Union[str, int, date, datetime, Timestamp]
