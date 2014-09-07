#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
#    workspace.py
#
#    Copyright (c) 2014
#    Author: Claudio Driussi <claudio.driussi@gmail.com>
#
from sqlalchemy.ext.declarative import declarative_base
from .db import *

class WorkSpace(object):
    """Encapsulate an whole SQLAlchemy orm from an DynaQ db definition object"""

    def __init__(self, db, engine):
        """init the workspace

        :param db: DynaQ db definition object
        :param engine: SQLAlchemy engine string
        :return: None
        """
        self.db = db
        self.engine = engine
        self.metadata = sa.MetaData()
        self.Base = declarative_base(self.engine, self.metadata)
        self.tables = {}

    def generate_orm(self, prefix='', pref_tabels={}, defaults={}):
        """Generate the SQLAlchemy orm objects

        the objects are stored in self.tables dictionary

        :param prefix: an optional prefix for table names ie: if the table
         name is "user" and prefix is "data_" the name become "data_user"
        :param pref_tabels: an optional dict for prefix bypass names for
         singles names ie: if pref_tabels is {'zip_codes': ''} the name of
         zip table is "zip_codes" even if prefix is "data_"
        :param defaults: function for handle default values (not handled yet)
        :return: an self.sa_obj() objet for convenient handle of orm classes
        """
        # build objects
        self.tables = {}
        for table in self.db.tables.values():
            self.tables[table.alias] = \
                type(table.name.capitalize(),(self.Base,),
                     self._set_table(table, prefix, pref_tabels, defaults))
        # build relations
        for alias in self.tables:
            self._set_retations(alias)
        return self.sa_obj()

    def _set_table(self, table, prefix='', pref_tabels={}, defaults={}):
        """Create a SQLAlchemy class object

        This private method called from self.generate_orm method is the core
        for SQLAlchemy objects creation, all supported features are implemented
        here.

        :param table: the DynaQ table for class generation
        :param prefix: same of generate_orm
        :param pref_tabels: same of generate_orm
        :param defaults: same of generate_orm
        :return: the class object for the table
        """
        def get_name(tname):
            s = tname.replace('_'+USRFLD_KEY,'')
            pref = pref_tabels[s] if s in pref_tabels else prefix
            return pref + tname
        table_data = {}
        table_data['__tablename__'] = get_name(table.name)
        table_data['__dqt__'] = table
        for f in table.fields:
            foreignkey = None
            if isinstance(f.type, Table):
                foreignkey = "%s.%s" % (get_name(f.type.name), f.type.key.name)
            db_type = f.get_type()
            sa_type = db_type.sa_type
            if db_type.length and sa_type in [sa.Numeric, sa.Float]:
                sa_type = sa_type(db_type.length, f.get('decimals'))
            if  db_type.length and sa_type in [sa.String, sa.String, sa.CHAR, sa.LargeBinary, sa.Text,]:
                sa_type = sa_type(db_type.length)
            if foreignkey:
                c = sa.Column(sa_type, sa.ForeignKey(foreignkey))
            else:
                c = sa.Column(sa_type, primary_key=f == table.key)
            c.__dqf__ = f
            default = defaults.get(f.get('default'), None)
            if default:
                c.ColumnDefault(default)
            table_data[f.name] = c
        ii = []
        for i in table.indexes:
            if i.name == 'primary':
                continue
            ii.append(sa.Index('idx_%s_%s' % (table.alias, i.name), *i.fields))
        # if needed add more table args
        if ii:
            table_data['__table_args__'] = tuple(ii)
        return table_data

    def _set_retations(self, alias):
        """Create the orm relationships.

        This private method called from self.generate_orm method generate the
        one to many relations for each related field of the table pointed by
        alias parameter. It handle "cascade referential integrity" if the
        related field has the property "child == True"

        :param alias: alias name of the table
        :return: None
        """
        for field in self.db.tables[alias].fields:
            if field.get('child'):
                parent = self.tables[field.type.alias]
                child = self.tables[alias]
                setattr(parent, alias,
                        sa.orm.relationship(child,
                                backref=parent.__tablename__,
                                cascade="all, delete, delete-orphan"))

    def sa_obj(self):
        """Build a convenient object for accessing to SqlAlchemy ORM objects

        Example:
        ws = dq.WorkSpace(db, engine)
        ws.generate_orm()
        o = ws.sa_obj()

        now if in your definition is a table called users, you can do:
        user = o.users()
        :return: the container object
        """
        t = {}
        for k,v in self.tables.items():
            t[k] = v
        return type('WSO', (object,), t)

    def session(self):
        """Return a session instance for the workspace"""
        return sa.orm.sessionmaker(bind=self.engine)()

