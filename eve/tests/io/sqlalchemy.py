# -*- coding: utf-8 -*-

from datetime import datetime
from unittest import TestCase

from eve import Eve
from eve.io.sqlalchemy.parser import parse, ParseError
from eve.io.sqlalchemy import Validator
from cerberus.errors import ERROR_BAD_TYPE
from eve.io.sqlalchemy import SQLAlchemy


app = Eve(data=SQLAlchemy)

db = app.data.driver

test_data = [
    (u'Barack', u'Obama', datetime(1961, 8, 4))
]


class People(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    firstname = db.Column(db.String(80))
    lastname = db.Column(db.String(120))
    born = db.Column(db.DateTime())
    fullname = db.column_property(firstname + " " + lastname)

    def __repr__(self):
        return '<People %s>' % (self.fullname,)

    @classmethod
    def from_tuple(cls, data):
        return cls(firstname=data[0], lastname=data[1], born=data[2])


db.create_all()
if not db.session.query(People).count():
    people = [People.from_tuple(item) for item in test_data]
    db.session.add_all(people)
    db.session.commit()


def _from_sqla_query_to_string(sqla_query):
    sql_compiled = sqla_query.compile()
    sql_compiled.visit_bindparam = sql_compiled.render_literal_bindparam
    return sql_compiled.process(sql_compiled.statement)


class TestPythonParser(TestCase):

    def test_Eq(self):
        r = parse('firstname == "whatever"', People)
        self.assertEqual(type(r), list)
        sql = _from_sqla_query_to_string(r.pop())
        self.assertEqual(sql, "people.firstname = 'whatever'")

    def test_Gt(self):
        r = parse('firstname > 1', People)
        self.assertEqual(type(r), list)
        sql = _from_sqla_query_to_string(r.pop())
        self.assertEqual(sql, 'people.firstname > 1')

    def test_GtE(self):
        r = parse('firstname >= 1', People)
        self.assertEqual(type(r), list)
        sql = _from_sqla_query_to_string(r.pop())
        self.assertEqual(sql, 'people.firstname >= 1')

    def test_Lt(self):
        r = parse('firstname < 1', People)
        self.assertEqual(type(r), list)
        sql = _from_sqla_query_to_string(r.pop())
        self.assertEqual(sql, 'people.firstname < 1')

    def test_LtE(self):
        r = parse('firstname <= 1', People)
        self.assertEqual(type(r), list)
        sql = _from_sqla_query_to_string(r.pop())
        self.assertEqual(sql, 'people.firstname <= 1')

    def test_NotEq(self):
        r = parse('firstname != 1', People)
        self.assertEqual(type(r), list)
        sql = _from_sqla_query_to_string(r.pop())
        self.assertEqual(sql, 'people.firstname != 1')

    def test_And_BoolOp(self):
        r = parse('firstname == 1 and lastname == 2', People)
        self.assertEqual(type(r), list)
        sql = _from_sqla_query_to_string(r.pop())
        self.assertEqual(sql,
                         'people.firstname = 1 AND people.lastname = 2')

    def test_Or_BoolOp(self):
        r = parse('firstname == 1 or lastname == 2', People)
        self.assertEqual(type(r), list)
        sql = _from_sqla_query_to_string(r.pop())
        self.assertEqual(sql, 'people.firstname = 1 OR people.lastname = 2')

    def test_nested_BoolOp(self):
        r = parse('firstname == 1 or (lastname == 2 and born == 3)', People)
        self.assertEqual(type(r), list)
        sql = _from_sqla_query_to_string(r.pop())
        self.assertEqual(sql,
                         'people.firstname = 1 OR people.lastname = 2'
                         ' AND people.born = 3')
    # TODO: implement visit_Call in eve/io/sqlalchemy/parser.py
    # def test_datetime_Call(self):
    #     r = parse('born == datetime(1961, 8, 4)', People)
    #     self.assertEqual(type(r), list)
    #     sql = _from_sqla_query_to_string(r.pop())
    #     self.assertEqual(sql, {'born': datetime(1961, 8, 4)})

    def test_Attribute(self):
        r = parse("firstname == 'Obama'", People)
        self.assertEqual(type(r), list)
        sql = _from_sqla_query_to_string(r.pop())
        self.assertEqual(sql,
                         "people.firstname = 'Obama'")

    # TODO: at the moment returns [], should return ParseError?
    # def test_unparsed_statement(self):
    #     self.assertRaises(ParseError, parse, 'print "hello"', People)

    def test_bad_Expr(self):
        self.assertRaises(ParseError, parse, 'a | 2', People)


# TODO: Include also these tests
# class TestSQLAlchemyValidator(TestCase):
#     def test_unique_fail(self):
#         """ relying on POST and PATCH tests since we don't have an active
#         app_context running here """
#         pass
#
#     def test_unique_success(self):
#         """ relying on POST and PATCH tests since we don't have an active
#         app_context running here """
#         pass
#
#     def test_objectid_fail(self):
#         schema = {'id': {'type': 'objectid'}}
#         doc = {'id': 'not_an_object_id'}
#         v = Validator(schema, People)
#         self.assertFalse(v.validate(doc))
#         self.assertTrue(ERROR_BAD_TYPE % ('id', 'ObjectId') in
#                         v.errors)
#
#     def test_objectid_success(self):
#         schema = {'id': {'type': 'objectid'}}
#         doc = {'id': '50656e4538345b39dd0414f0'}
#         v = Validator(schema, None)
#         self.assertTrue(v.validate(doc))
#
#     def test_transparent_rules(self):
#         schema = {'a_field': {'type': 'string'}}
#         v = Validator(schema)
#         self.assertTrue(v.transparent_schema_rules, True)
