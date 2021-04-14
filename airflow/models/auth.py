# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pendulum import from_timestamp
from sqlalchemy import Column, DateTime, Integer, String

from airflow.models.base import Base
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session


class TokenBlockList(Base, LoggingMixin):
    """
    A model for recording blocked token

    :param jti: The token jti(JWT ID)
    :type jti: str
    :param expiry_date: When the token would expire
    :type expiry_date: DateTime
    """

    __tablename__ = 'token_blocklist'
    id = Column(Integer, primary_key=True)
    jti = Column(String(50), unique=True, nullable=False)
    expiry_date = Column(DateTime, nullable=False, index=True)

    def __init__(self, jti: str, expiry_date: DateTime):
        super().__init__()
        self.jti = jti
        self.expiry_date = expiry_date

    @classmethod
    @provide_session
    def get_token(cls, token, session=None):
        """Get a token"""
        tkn = session.query(cls).filter(cls.jti == token).first()
        return tkn

    @classmethod
    @provide_session
    def delete_token(cls, token, session=None):
        """Delete a token"""
        tkn = session.query(cls).filter(cls.jti == token).first()
        if tkn:
            session.delete(tkn)
            session.commit()

    @classmethod
    @provide_session
    def add_token(cls, jti, expiry_delta, session=None):
        """Add a token to blocklist"""
        token = cls(jti=jti, expiry_date=from_timestamp(expiry_delta))
        session.add(token)
        session.commit()
