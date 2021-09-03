import os
from flask import Flask

app = Flask(__name__)

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # a simple page that says hello
    #@app.route('/hello')
    #def hello():
    #    return 'Hello, World!'
    
    from flaskr import dashboard_web
    return app

