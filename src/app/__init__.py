from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://flask:password@localhost/integrador-api-service'
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.secret_key = 'SECRET_KEY001'

db = SQLAlchemy(app)

