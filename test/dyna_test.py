#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Test of dynamic creation of SQLAlchemy objects.
#
# Here I experiment some techniques used to generate workspace object
#

import os
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

engine = sa.create_engine('sqlite:///temp.db', echo = False)
metadata = sa.MetaData()
Base = declarative_base(engine, metadata)
Session = sa.orm.sessionmaker(bind=engine)


# dynamically create sa classes
d = dict()

# create users

# ndx = (sa.Index('idx_usr_name', 'name'), sa.Index('idx_usr_fullname', 'fullname'),{'schema':'abc'},)
l = ['name', 'password']
ndx = [sa.Index('idx_usr_name', *l), sa.Index('idx_usr_fullname', 'fullname'), ]
ndx = tuple(ndx)

def defval(t):
    return 10

x = sa.Column(sa.Numeric(10,3))
x.default = sa.ColumnDefault(defval)

id = sa.Column(sa.Integer, primary_key=True)
id.__dqf__ = 'id'

t = {'__tablename__': "users",
     'id': id,
     'name': sa.Column(sa.String),
     'fullname': sa.Column(sa.String),
     'password': sa.Column(sa.String),
     'cost': x,

     '__table_args__': ndx,
     '__dqt__': 'usr',
}
d['usr'] = type('User',(Base,),t)
def f(self):
    return "User: %d - %s " % (self.id, self.name)
d['usr'].__repr__ = f
# sa.Index('idx_usr_name','users','name')


# create addresses
def f(self):
    return "Address: %d - %s " % (self.user_id, self.email_address)
t = {'__tablename__': 'addresses',
     'id': sa.Column(sa.Integer, primary_key=True),
     'email_address': sa.Column(sa.String, nullable=False),
     'user_id': sa.Column(sa.Integer, sa.ForeignKey('users.id')),
     '__repr__': f
}
d['adr'] = type('Address',(Base,),t)

# bind relationships with cascade clean up
#d['usr'].addresses = sa.orm.relationship(d['adr'], backref="users", cascade="all, delete, delete-orphan")
setattr(d['usr'], 'addresses', sa.orm.relationship(d['adr'], backref="users", cascade="all, delete, delete-orphan"))


if __name__ == '__main__':
    metadata.create_all()

    # add a user with 2 addresses
    s = Session()
    x = d['usr']()
    x.name = 'claudio'
    x.fullname = 'io'
    x.password = 'claudio'
    x.addresses.append(d['adr'](email_address="info@mysite.com"))
    x.addresses.append(d['adr'](email_address="claudio@mysite.com"))
    s.add(x)
    s.commit()
    id = x.id
    print x
    print x.cost
    print x.addresses

    # count created records
    usr = d['usr']
    print s.query(usr).filter_by(fullname='io').count()
    print s.query(d['adr']).filter_by(user_id=id).count()

    # delete with cascade effect
    x = s.query(usr).filter_by(fullname='io').one()
    s.delete(x)
    s.commit()

    # count remaining records
    print s.query(usr).filter_by(fullname='io').count()
    print s.query(d['adr']).filter_by(user_id=id).count()
    print d['usr'].id.key
    print d['usr'].__dqt__
