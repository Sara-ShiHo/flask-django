import traceback
import logging.config

from flask import Flask
from flask import render_template

from src.db import News, NewsManager
ENGINE_STRING = 'sqlite:///data/news.db'

logger = logging.getLogger(__name__)
logging.config.fileConfig("config/local.conf",
                          disable_existing_loggers=False)


# Initialize the Flask application
app = Flask(__name__,
            template_folder="app/templates",
            static_folder="app/static")

manager = NewsManager(engine_string=ENGINE_STRING)
db_session = manager.session


@app.route('/')
def index():
    try:
        news_entities = db_session.query(News).all()

        logger.debug("Index page accessed")
        return render_template('index.html',
                               date='06-04-2021',
                               news_entities=news_entities)

    # should handle *any* exceptions to avoid front-end errors in deployed app
    except:
        logger.debug("session.rollback() invoked")
        db_session.rollback()

        traceback.print_exc()
        logger.warning("Not able to display wikinews, error page returned")
        return render_template('index.html')


if __name__ == '__main__':
    app.run()
