#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# db.py
#
# Copyright (c) 2014
# Author: Claudio Driussi <claudio.driussi@gmail.com>
#
import sqlalchemy as sa
import copy

# sqlalchemy recognized types
base_types = {
    'integer': sa.Integer,
    'autoinc': sa.Integer,
    'bool': sa.Boolean,
    'char': sa.CHAR,
    'varchar': sa.String,
    'blob': sa.LargeBinary,
    'text': sa.Text,
    'date': sa.Date,
    'datetime': sa.DateTime,
    'numeric': sa.Numeric,
    'float': sa.Float
}

# table kinds
table_kinds = ['anag', 'mov', 'tab', 'child']
TK_ANAG = 0
TK_MOV = 1
TK_TAB = 2
TK_CHILD = 3

USRFLD_KEY = 'usrfld'
USRFLD_SUFFIX = '_uf'


class PropContainer():
    """
    PropContainer is the base class for all DynaQ db objects and is used to
    handle properties.
    It has a single method which return a value from self.properties dict.
    If the property is not found search in the instance variables too.
    """
    def __init__(self):
        """init the properties dict"""
        self.properties = {}

    def get(self, key, default=None):
        """Get the value of a property

        First search in self.properties dict and if not found search in the
        instance variables too, if is not found again, return the default value.

        :param key: key value to search
        :param default: default value if not found
        :return: the found value or default
        """
        if key in self.properties:
            return self.properties[key]
        return self.__dict__.get(key, default)


class Database(PropContainer):
    """
    This object contain a whole database definition.
    The definition usually is stored in yaml hierarchy of files and are loaded
    with the load_yaml() method.

    The core concept of the system is that each object has properties that your
    application can access and use, some properties are used for generate
    SQLAlchemy orm classes and relations, other will be used for attached GUI
    systems or for other facilities.

    A Database is composed by Tables and Types. Tables are the tables of the
    database that are mapped into SQLAlchemy objects. Types are abstract types
    organized in a tree hierarchy, at the root types are mapped into
    SQLAlchemy types. The types have properties and the child types inherit
    properties of ancestors.
    """
    def __init__(self):
        """init variables"""
        PropContainer.__init__(self)
        self.name = ''
        self.types = {}
        self.tables = {}

    def load_yaml(self, data):
        """ Build Database structure from a dict loaded from yaml files

        data is a dict loaded from a yaml files following the defined rules.
        The utils.YamlLoader class is used to load files with includes and
        precedences rules. Example:
        #>>> import dynaq as dq
        #>>> YAML_DIR = "yaml"
        #>>> YPATH = [os.path.join(YAML_DIR, "custom"), YAML_DIR,
                     os.path.join(YAML_DIR, "core")]
        #>>> yaml = dq.utils.YamlLoader(open(os.path.join(YAML_DIR, "db.yml")
                    ,'r'),YPATH).get_data()
        #>>> db = dq.Database()
        #>>> db.load_yaml(yaml)

        :param data: dict loaded from yaml files
        :return: None
        """
        if data['type'] != 'db':
            raise Exception('Incorrect database script: %s' % data['type'])
        if not self.name:
            self.name = data['name']
        # skip handled properties
        for k,v in list(data.items()):
            if k in ['type', 'name', 'types', 'tables', 'properties']:
                continue
            self.properties[k] = v

        if 'types' in data:
            for t in data['types']:
                self.add_types(t)
            self.calc_types()

        if 'tables' in data:
            for t in data['tables']:
                self.add_table(t)
            # properties of tables at database level
            for i in add_properties(data, 'tables', ):
                self.tables[i[0]].properties[i[1]] = i[2]
            self.calc_tables()

    def add_types(self, data):
        """Add types definitions to Database object.

        Each type is a list in the form of [name, inherit, length, decimals,
        inline properties only "name" is required, the last
        element can be a dict with inline properties
        :param data: dict containing type definitions
        """
        if data['type'] != 'types':
            raise Exception('Incorrect types script: %s' % data['type'])
        if not 'types' in data:
            return
        for tt in data['types']:
            t = Type(tt[0])
            for i, v in enumerate(tt):
                if type(v) is dict:
                    t.properties = v
                    break
                if i == 1:
                    t.inherit = v
                if i == 2:
                    t.length = v
                if i == 3:
                    t.properties['decimals'] = v
            self.types[t.name] = t
        # if properties are defined, add them to the types
        for i in add_properties(data, 'types', ):
            self.types[i[0]].properties[i[1]] = i[2]

    def calc_types(self):
        """ Calc types properties and SqlAlchemy base types

        Recursively navigate into all types stored in self.types dictionary
        until the root, assign SQLAlchemy type to root and to all derived types
        and assign properties from root to derived types. This method can be
        called from outside if types are added at runtime

        :return: None
        """
        for k, v in list(self.types.items()):
            def _set_type(t, tb=None):
                if t.fields:
                    return
                elif not tb:
                    t.sa_type = base_types[t.name]
                else:
                    tb = self.types[tb]
                    _set_type(tb, tb.inherit)
                    t.sa_type = tb.sa_type
                    if not t.length:
                        t.length = tb.length
                    if not t.fields:
                        t.fields = tb.fields
                    for k, v in list(tb.properties.items()):
                        if not k in t.properties:
                            t.properties[k] = v

            v.calc_properties()
            _set_type(v, v.inherit)

    def add_table(self, data):
        """Add a table to the list of tables of the database

        :param data: is the dict containing the Table definition
        :return: None
        """
        if data['type'] != 'table':
            raise Exception('Incorrect table script: %s - %s' % (data['type'], data['name']))
        if not 'fields' in data:
            raise Exception('Fields are required in table script: %s' % data['name'])
        if 'inherit' in data:
            t = copy.deepcopy(self.tables[data['inherit']])
            t.set_names(self, data['name'], data.get('alias',''))
        else:
            t = Table(self, data['name'], data.get('alias',''))
        t.calc_dict(data)
        self.tables[t.alias] = t

    def calc_tables(self):
        """Resolve relations and user fields

        Resolve relations between tables assigning the relate table to the
        type of the related field. So if the filed is instance of Type object
        is an ordinary field and if is instance of Table object i a related
        field into the pointed Table and the type of field is determined by
        the "key" var of the related table.

        If the table has the property "usrvar", will be created a table to
        store "user variables" for each record of the main table.

        This method can be called more then once to recalc the database if
        Tables are added at runtime.

        :return: None
        """
        # user fields generation
        uf = []
        for k,v in list(self.tables.items()):
            if v.properties.get(USRFLD_KEY,False):
                d = {'type': 'table',
                     'name': '%s_%s' % (v.name, USRFLD_KEY) ,
                     'alias': '%s%s' % (v.alias, USRFLD_SUFFIX),
                     'kind': 'child',
                     'title': 'User fields for %s' % v.get('title'),
                     'fields': [
                         ['id', 'idint', 'User fields ID'],
                         ['id_%s' % v.alias, '=%s' % v.alias, '%s' % v.get('title'), {'child': True}],
                         ['name', 'idname', 'Variable name'],
                         ['value', 'text', 'Variable value']
                     ],
                     'indexes': [
                         ['id_%s' % v.alias, ['id_%s' % v.alias,'name'], 'Variable']
                     ]
                     }
                uf.append(d)
        for t in uf:
            self.add_table(t)
        # resolve relations.
        # at the moment primary keys uses only one column, maybe in the
        # future we will add logic to handle multiple columns primary keys
        for k,v in list(self.tables.items()):
            for f in v.fields:
                if type(f.type) is str:
                    f.type = self.tables[f.type[1:]]


class Type(PropContainer):
    """
    This object contain a single type definition. Each type can inherit
    properties from the ancestors.
    There are 2 special compound types:
    The array type is identified by property "array" and at runtime generates
    as many fields as indicated by with the name followed by the ordinal number
    ie: if the name of field is "discount" and the array property has value 5
    will be generated the fields: "discount01", "discount02", "discount03",
    "discount04" and "discount05"
    The compound type is identified by property "fields" which store the fields
    contented at runtime generates all fields with the name of field followed
    by the name of fields contained, ie: if the name of field is "add_" and
    the fields are ["street", "city", "zip"], the generated field are
    "add_street", "add_city" and "add_zip"
    """
    def __init__(self, name):
        """Init the Type object

        :param name: name of type
        :return: None
        """
        PropContainer.__init__(self)
        self.sa_type = None
        self.name = name
        self.inherit = None
        self.length = 0
        self.fields = []

    def calc_properties(self):
        """ transform known properties to fields """
        def _to_field(t, prop):
            if prop in t.properties:
                setattr(t, prop, t.properties[prop])
                del t.properties[prop]
        _to_field(self, 'length')
        _to_field(self, 'fields')


class Table(PropContainer):
    """
    This object contain a definition of a single table.

    The table is composed by fields and indexes and has some other vars:

    db: is the reference to the database
    name: is the name of the table into physical database
    alias: is a short name for the table and is used as key for tables
      dictionaries, by default is the same of name
    key: is the reference of field used as primary key
    fnames and inames are dict used to find field and indexes by name
    """
    def __init__(self, db, name, alias=''):
        """Init the Tablle object

        :param db: reference to container db
        :param name: name of table
        :param alias: optional alias for table name
        :return: None
        """
        PropContainer.__init__(self)
        self.set_names(db, name, alias)
        self.key = None
        self.fields = []
        self.fnames = {}
        self.indexes = []
        self.inames = {}

    def set_names(self, db, name, alias=''):
        """Set names for table, splitted from __initi__ to handle inheritance

        :param db: reference to container db
        :param name: name of table
        :param alias: optional alias for table name
        :return: None
        """
        self.db = db
        self.name = name
        self.alias = alias
        if not alias:
            self.alias = name


    def calc_dict(self, data):
        """Build the table values using a dict read from yaml file.

        :param data: the dict containing definitions
        :return: None
        """
        # skip handled properties
        for k,v in list(data.items()):
            if k in ['type', 'name', 'alias', 'fields', 'indexes', 'properties']:
                continue
            self.properties[k] = v

        # set kind of table
        self.properties.setdefault('kind','anag')
        self.properties['kind'] = table_kinds.index(self.properties['kind'])

        # add fields
        for f in data['fields']:
            self._add_field(f)
        for i in add_properties(data, 'fields', ):
            self.fnames[i[0]].properties[i[1]] = i[2]

        # if there are no indexes declared the primary key is the first field
        if not 'indexes' in data:
            data['indexes'] = []
        # one and only one primary key per table
        primary = 0
        for i in data['indexes']:
            if i[0] == 'primary':
                primary += 1
        if primary > 1:
            raise Exception('More then one primary key in "%s" table script' % data['name'])
        if not primary:
            data['indexes'].insert(0, ['primary', data['fields'][0][0], data['fields'][0][2]])
        for i in data['indexes']:
            self._add_index(i)
        for i in add_properties(data, 'indexes', ):
            self.inames[i[0]].properties[i[1]] = i[2]

    def _add_field(self, field, compound=False):
        """Private method used to add a single field to the table.

        Add a field to the table. If the type field is a composed type, add
        all fields provided by Type rules. If the type is a reference to a
        table, is leaved as is and will be resolved later.

        At the moment compound fields cannot be nested so we can't have array
        of fields or field of arrays and so on.

        :param field: is the list containing the data in form of
        [name, type, description, properties], properites is an optional dict.
        :param compound: is True if called recursively for compound fields.
        :return: None
        """
        if field[1][0] == '=':
            ft = field[1]
        else:
            if not field[1] in self.db.types:
                raise Exception('Type "%s" of field "%s" not defined in table "%s"' % (field[1], field[0], self.name))
            ft = self.db.types[field[1]]
            if not compound:
                fname = field[0]
                if 'array' in ft.properties:
                    ff = copy.deepcopy(field)
                    for i in range(ft.properties['array']):
                        ff[0] = '%s%02d' % (fname, i + 1)
                        self._add_field(ff, True)
                    ft = None
                elif ft.fields:
                    for i in ft.fields:
                        ff = copy.deepcopy(i)
                        ff[0] = '%s%s' % (fname, i[0])
                        self._add_field(ff, True)
                    ft = None
        if ft:
            f = Field(self, field[0], ft)
            f.description = field[2]
            if len(field) > 3:
                f.properties = field[3]
            self.fields.append(f)
            self.fnames[f.name] = f

    def _add_index(self, index):
        """Private method used to add a single index to the table.

        Add an index to the table. Primary Key and Foreign Keys are handled
        automatically. The primary key is identified by the name "primarykey",
        must be composed by a single field and if not present is assigned to
        the first field of table

        :param index: is the list containing data in form of
        [name, fields, description, properties], properties is an optional dict.
        :return: None
        """
        i = Index(self, index[0], index[1])
        i.description = index[2]
        if len(index) > 3:
            i.properties = index[3]
        self.indexes.append(i)
        self.inames[i.name] = i
        if i.name == 'primary':
            self.key = self.fnames[i.fields[0]]


class Field(PropContainer):
    """
    Field definition object.

    The field has his own properties, but inherit properties for his Type and
    if is a related field inherit properties form key field of the related
    table.
    """
    def __init__(self, table, name, type_):
        """Init the Field object

        :param table: reference to the Table objet
        :param name: name of field
        :param type_: Type of field or related table
        :return: None
        """
        PropContainer.__init__(self)
        self.table = table
        self.name = name
        self.type = type_
        self.description = ''

    def get(self, key, default=None):
        """Get a property of a field.

        If not found return the property of the type of field, if is a related
        field search also in the key of related table.

        :param key: key to search
        :param default: default value if not found
        :return: the property value or default
        """
        v = PropContainer.get(self, key)
        if v:
            return v
        t = self.get_type()
        return t.get(key, default)

    def get_type(self):
        """Return the type of field and if is a related table return the
        type of the related key field

        :return: the type of field
        """
        return self.type.key.type if isinstance(self.type, Table) else self.type


class Index(PropContainer):
    """
    This object store optionals indexes of Tables.
    """
    def __init__(self, table, name, fields):
        """Init the index object
        :param table: reference to the Table objet
        :param name: name of the index
        :param fields: list of the indexed fields, if a string is passed, it
          is transformed into a list.
        :return: None
        """
        PropContainer.__init__(self)
        self.table = table
        self.name = name
        if type(fields) is str:
            fields = [fields]
        self.fields = fields
        self.description = ''
        self.properties = {}


def add_properties(data, key):
    """Generator function for properties of each yaml file

    If a yaml file has the key "properties" and properties has the key "key",
    yield the content of key.
    yaml example:
    properties:
     fields:
      - [description, length, 128]

    :param data: dict loaded from yaml
    :param key: key properties to search
    :return: None
    """
    if 'properties' in data and key in data['properties']:
        for i in data['properties'][key]:
            assert isinstance(i, list)
            yield i
