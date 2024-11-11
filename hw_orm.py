# import sqlalchemy
import os
import json
import sqlalchemy as sq_al
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import dotenv

def load_dotenv():
    """Функция, обеспечивающая получение данных из файла окружения."""
    dotenv.load_dotenv()
    dotenv.load_dotenv(dotenv.find_dotenv(filename='config.env', usecwd=True))

def get_shops(idf):
    """
    Функция для вывода продаж по издателям.
    """

    q_temp =  (session.query(Book, Publisher, Shop, Sale).
          join(Publisher).join(Stock, Book.id == Stock.id_book).
          join(Shop, Shop.id == Stock.id_shop).
          join(Sale, Stock.id == Sale.id_stock)
          )
    if idf.isdigit():
        q_finish= q_temp.filter(Publisher.id == idf).all()
    else:
        q_finish = q_temp.filter(Publisher.name == idf).all()


    print('-' * 80)
    for s in q_finish:
        print(
            f'| {s.Book.title.ljust(40)}  |  {s.Shop.name.ljust(20)} |{str(s.Sale.count * s.Sale.price).rjust(10)}|{s.Sale.date_sale.strftime('%d-%m-%Y')}')


def read_from_json(file_path):
    """
    Функция для чтения файла json.
    """
    with open(file_path) as f:
        json_data = json.load(f)
        for lst in json_data:
            lvl_1 = lst.get('model')
            match lvl_1:
                case 'publisher':
                    print(lst['pk']," | ",lst['fields']['name'])
                    pub = Publisher(id=lst['pk'], name=lst['fields']['name'])
                case 'book':
                    pub = Book(id=lst['pk'], title=lst['fields']['title'], id_publisher=lst['fields']['id_publisher'])
                case 'shop':
                    pub = Shop(id=lst['pk'], name=lst['fields']['name'])
                case 'stock':
                    pub = Stock(id=lst['pk'], id_book=lst['fields']['id_book'], id_shop=lst['fields']['id_shop'],
                                count=lst['fields']['count'])
                case 'sale':
                    pub = Sale(id=lst['pk'], price=lst['fields']['price'], date_sale=lst['fields']['date_sale'],
                               id_stock=lst['fields']['id_stock'], count=lst['fields']['count'])

                case _:
                    print(" что-то непонятное болтается в jsone")
            session.add(pub)
            session.commit()


def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

Base = declarative_base()

class Publisher(Base):
    __tablename__ = "publisher"
    id = sq_al.Column(sq_al.Integer, primary_key=True)
    name = sq_al.Column(sq_al.String(length=60), unique=False)

class Shop(Base):
    __tablename__ = "shop"
    id = sq_al.Column(sq_al.Integer, primary_key=True)
    name = sq_al.Column(sq_al.String(length=60), nullable=False)

class Book(Base):
    __tablename__ = "book"
    id = sq_al.Column(sq_al.Integer, primary_key=True)
    title = sq_al.Column(sq_al.Text, nullable=False)
    id_publisher = sq_al.Column(sq_al.Integer, sq_al.ForeignKey("publisher.id"), nullable=False)

class Stock(Base):
    __tablename__ = 'stock'
    id = sq_al.Column(sq_al.Integer, primary_key=True)
    id_book = sq_al.Column(sq_al.Integer, sq_al.ForeignKey('book.id'), nullable=False)
    id_shop = sq_al.Column(sq_al.Integer, sq_al.ForeignKey('shop.id'), nullable=False)
    count = sq_al.Column(sq_al.Integer, nullable=False)
    book = relationship(Book, backref="stock1")
    shop = relationship(Shop, backref="stock2")


class Sale(Base):
    __tablename__ = 'sale'
    id = sq_al.Column(sq_al.Integer, primary_key=True)
    price = sq_al.Column(sq_al.Float, nullable=False)
    date_sale = sq_al.Column(sq_al.DateTime, nullable=False)
    id_stock = sq_al.Column(sq_al.Integer, sq_al.ForeignKey('stock.id'), nullable=False)
    count = sq_al.Column(sq_al.Integer, nullable=False)
    stock = relationship(Stock, backref="sale")


if __name__ == '__main__':
    load_dotenv()
    user = os.getenv("USER")
    passw = os.getenv("PASSWORD")
    port = os.getenv("PORT")
    base = os.getenv("BASENAME")
    host = os.getenv("HOST")

    DSN = f'postgresql://{user}:{passw}@{host}:{port}/{base}'
    print(DSN)
    print('-'*80)

    engine = sq_al.create_engine(DSN)
    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    read_from_json('data.json')
    idf = input("Введите идентификатор или наименование издателя из списка выше >>")

    get_shops(idf)

    session.close()
