#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
#       test_load.py
#
#       Copyright (c) 2014
#       Author: Claudio Driussi <claudio.driussi@gmail.com>

import os
import unittest
import dynaq as dq
import sqlalchemy as sa


# directories of yaml files. YPATH are optional dirs used to search include
# files, so we can override standard yaml files with custom ones.
YAML_DIR = "yaml"
YPATH = [YAML_DIR, os.path.join(YAML_DIR, "custom"), os.path.join(YAML_DIR, "core")]

class LoadTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(LoadTest, self).__init__(*args, **kwargs)
        # load yaml file with all includes
        self.yaml = dq.utils.YamlLoader(open(os.path.join(YAML_DIR, "db.yml"),'r'),YPATH).get_data()
        self.custom = dq.utils.YamlLoader(open(os.path.join(YAML_DIR, "custom","custom_db.yml"),'r'),YPATH).get_data()

    def test_loader(self):
        # yaml dict has types and tables
        self.failUnless('types' in self.yaml)
        self.failUnless('tables' in self.yaml)
        # a table file has the key 'type' which identify the type of file
        self.failUnless('type' in self.yaml['tables'][0])

    def test_db_yaml(self):
        # instantiate a Database class and load the yaml files
        db = dq.Database()
        db.load_yaml(self.yaml)
        # load custom tables and add them to the Database
        db.load_yaml(self.custom)
        # add type at runtime
        t = dq.Type('mytype')
        t.inherit = 'quantity'
        t.length = 10
        db.types['mytype'] = t
        # recalc the types after runtime type add
        db.calc_types()

        # some types assertions, see yaml files
        self.failUnless(db.types)
        self.failUnless('default' in db.types['today'].properties)
        self.failUnless(db.types['today'].get('default'))
        self.failUnless(db.types['currency'].sa_type == sa.Numeric)
        self.failUnless(db.types['price'].get('decimals'))
        self.failUnless(db.types['mytype'].length == 10)
        self.failUnless('description' in db.types['name'].properties)

        # fields in tables
        # measure custom table
        self.failUnless('msr' in db.tables)
        # some field assertions
        self.failUnless('name' in db.tables['sbj'].fnames)
        self.failUnless('discount01' in db.tables['prc'].fnames)
        self.failUnless('add_street' in db.tables['sbj'].fnames)
        self.failUnless('length' in db.tables['prd'].fnames['description'].properties)
        self.failUnless('color' in db.tables['cst'].properties)

        # indexes in tables
        # some indexes assertions
        self.failUnless('primary' in db.tables['tax'].inames)
        self.failUnless('primary' in db.tables['prd'].inames)
        self.failUnless('ascending' in db.tables['ord'].inames['date'].properties)
        self.failUnless(isinstance(db.tables['ord'].fnames['id_sbj'].type, dq.Table))

        # user fields
        self.failUnless('usrfld' in db.tables['sbj'].properties)
        self.failUnless('usrfld' in db.tables['prd'].properties)
        self.failUnless('sbj_uf' in db.tables)
        self.failUnless('prd_uf' in db.tables)
        self.failUnless(isinstance(db.tables['sbj_uf'].fnames['id_sbj'].type, dq.Table))


class WSTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(WSTest, self).__init__(*args, **kwargs)
        yaml = dq.utils.YamlLoader(open(os.path.join(YAML_DIR, "db.yml"),'r'),YPATH).get_data()
        self.db = dq.Database()
        self.db.load_yaml(yaml)
        if os.path.isfile('test.db'):
            os.remove('test.db')

    def test_wa(self):
        engine = sa.create_engine('sqlite:///test.db', echo=False)
        ws = dq.WorkSpace(self.db, engine)

        # generate ord with 'data_' table prefix, except for products table
        # which has no prefix. (usrfld tables keep the same prefix of the owner)
        o = ws.generate_orm('data_',{'products': ''})
        ws.metadata.create_all()

        # test the presence of DynaQ table reference
        self.failUnless(o.sbj.__dqt__.name == 'subjects')

        # add some data to database and test the one to many relation
        s = ws.session()
        x = o.sbj()
        x.name = 'John'
        x.sbj_uf.append(o.sbj_uf(name='var1',value=1))
        x.sbj_uf.append(o.sbj_uf(name='var2',value=2))
        s.add(x)
        s.commit()

        # assert the record creations
        self.failUnless(s.query(o.sbj).count() == 1)
        self.failUnless(s.query(o.sbj_uf).filter_by(id_sbj=x.id).count() == 2)

        # delete records and assert record deletion
        t = s.query(o.sbj).first()
        s.delete(t)
        s.commit()
        self.failUnless(s.query(o.sbj).count() == 0)
        self.failUnless(s.query(o.sbj_uf).filter_by(id_sbj=x.id).count() == 0)


def main():
    unittest.main()

if __name__ == '__main__':
    main()
