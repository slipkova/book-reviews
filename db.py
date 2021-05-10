from sqlalchemy import create_engine, Column, ForeignKey, UniqueConstraint, desc
from sqlalchemy.types import Float, String, Integer, DateTime, Enum, Text, BLOB, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import datetime

SQLITE = 'sqlite'
MYSQL = 'mysql'

Base = declarative_base()


class Genre(Base):
    __tablename__ = 'genres'

    name = Column(String(50), primary_key=True, unique=True)
    info = Column(String(20), nullable=True, unique=False)
    books = relationship("Book", back_populates="genre")


class Author(Base):
    __tablename__ = 'authors'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False, unique=False)
    surname = Column(String(50), nullable=False, unique=False)
    info = Column(String(20), nullable=True, unique=False)
    books = relationship("Book", back_populates="author")


class Book(Base):
    __tablename__ = 'books'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=False)
    genre_id = Column(String, ForeignKey('genres.name'))
    author_id = Column(Integer, ForeignKey('authors.id'))


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(100), nullable=False, unique=True)
    username = Column(String(20), nullable=False, unique=True)
    password = Column(String(50), nullable=False)
    image = Column(BLOB)


class Database:
    DB_ENGINE = {
        'sqlite': 'sqlite:///{DB}',
        'mysql': 'mysql+mysqlconnector://{USERNAME}:{PASSWORD}@localhost/{DB}'
    }

    def __init__(self, dbtype='sqlite', username='', password='', dbname='books.db'):
        dbtype = dbtype.lower()

        if dbtype in self.DB_ENGINE.keys():
            engine_url = self.DB_ENGINE[dbtype].format(DB=dbname, USERNAME=username, PASSWORD=password)
            self.engine = create_engine(engine_url, echo=False)
        else:
            print('DBType is not found in DB_ENGINE')

        Base.metadata.create_all(self.engine)

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def order_query(self, query, order):
        try:
            return query.order_by(order)
        except:
            print("attribute not found")
            return False

    def join_query(self, query, object_type):
        try:
            return query.join(object_type)
        except Exception as e:
            print(e.__traceback__.tb_next)
            return False

    def filter_query(self, query, by):
        try:
            return query.filter(by)
        except Exception as e:
            print(e.__traceback__.tb_next)
            return False

    def list_query(self, query):
        try:
            return query.all()
        except Exception as e:
            print(e.__traceback__.tb_next)
            return False

    def add_object(self, object):
        try:
            self.session.add(object)
            self.session.commit()
            return True
        except Exception as e:
            print(e.__traceback__.tb_next)
            return False

    def query(self, object_type):
        try:
            return self.session.query(object_type)
        except Exception as e:
            print(e.__traceback__.tb_next)
            return False

    def get_object_by_attr(self, object, by, attr, condition=lambda x, v: x.is_(v)):
        try:
            return self.list_query(self.filter_query(self.query(object), condition(getattr(object, by), attr)))
        except Exception as e:
            print(e.__traceback__.tb_next)
            return False

    def get_object_by_relation(self, object, rel, by=None, value=None, condition=lambda b, v: True):
        try:
            return self.list_query(self.filter_query(self.join_query(self.query(object), rel), condition(by, value)))
        except Exception as e:
            print(e.__traceback__.tb_next)
            return False

    def update(self):
        try:
            self.session.commit()
            return True
        except Exception as e:
            print(e.__traceback__.tb_next)
            return False

    def del_object(self, object, by, attr):
        try:
            obj = self.get_object_by_attr(object, by, attr)[0]
            self.session.delete(obj)
            self.session.commit()
            return True
        except Exception as e:
            print(e.__traceback__.tb_next)
            return False