import logging
import logging.config
import traceback

import pandas as pd
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, MetaData, Text, Table
from sqlalchemy.orm import sessionmaker
from flask_sqlalchemy import SQLAlchemy

logger = logging.getLogger(__name__)
logging.config.fileConfig("config/local.conf",
                          disable_existing_loggers=False)

Base = declarative_base()


class News(Base):
    """Create schema for news data"""

    __tablename__ = 'news'

    news_id = Column(Integer, primary_key=True)
    headline = Column(String(1000), unique=False, nullable=False)
    news = Column(Text(10000), unique=False, nullable=False)
    news_image = Column(String(1000), unique=False, nullable=False)
    news_url = Column(String(1000), unique=False, nullable=False)

    def __repr__(self):
        return '<News id %r>' % self.news_id


class NewsManager:
    """Configuration for ingesting data into database"""

    def __init__(self, app=None, engine_string=None):
        """
        Args:
            app (obj): flask app
            engine_string (str): engine string referring to database
        """
        if app:
            logger.info('using WikiNewsManager for app')
            self.db = SQLAlchemy(app)
            self.session = self.db.session
        elif engine_string:
            logger.info('using WikiNewsManager for db')
            engine = sqlalchemy.create_engine(engine_string)
            Session = sessionmaker(bind=engine)
            self.session = Session()
        else:
            raise ValueError("Need either an engine string",
                             "or a Flask app to initialize")

    def close(self) -> None:
        """Closes session"""
        self.session.close()

    def add_news(self,
                 news_id: int, headline: str, news: str,
                 img: str, url: str) -> None:
        """Seeds an existing database with news"""

        session = self.session
        news_record = News(news_id=news_id,
                           headline=headline,
                           news=news,
                           news_image=img,
                           news_url=url)
        session.add(news_record)
        session.commit()
        logger.debug("'%s' added to db with id %i",
                     news[0:20],
                     news_id)


def delete_if_exists(engine_string, table_name) -> None:
    """Deletes rows in table to make way for new daily data

    Args:
        engine_string (str): engine string referring to database
        table_name (str): name of table in database
    """
    engine = sqlalchemy.create_engine(engine_string)

    Session = sessionmaker(bind=engine)
    session = Session()

    metadata = MetaData()
    metadata.reflect(bind=engine)

    inspector = sqlalchemy.inspect(engine)
    if table_name in inspector.get_table_names():
        try:
            logger.debug('Try deleting rows from table %s', table_name)
            # table = Table(table_name, metadata, autoload_with=engine)
            # session.query(table).delete(synchronize_session=False)
            base = declarative_base()
            table = metadata.tables.get(table_name)
            if table is not None:
                logger.info(f'Deleting {table_name} table')
                base.metadata.drop_all(engine, [table], checkfirst=True)

            # session.commit()
            logger.debug('Successfully rows from  table %s', table_name)
        except:
            logger.warning('Could not delete rows from %s', table_name)
            traceback.print_exc()
            session.rollback()


def create_db(engine_string: str) -> None:
    """Create database from provided engine string
    sqlite or rds instance engine
    """

    engine = sqlalchemy.create_engine(engine_string)
    delete_if_exists(engine_string, 'news')
    Base.metadata.create_all(engine)


def ingest_news(news_df, engine_string) -> None:

    db_manager = NewsManager(app=None, engine_string=engine_string)
    for _, row in news_df.iterrows():
        news_id, headline, news, image, url = row
        db_manager.add_news(news_id, headline, news, image, url)
    db_manager.close()


if __name__ == '__main__':

    engine_string = 'sqlite:///data/news.db'
    news_df = pd.read_csv('data/06-04-21-news-entries.csv')

    create_db(engine_string)
    ingest_news(news_df, engine_string)
