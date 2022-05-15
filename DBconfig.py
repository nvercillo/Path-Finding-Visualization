from flask import Flask, render_template, json
from models import db   

# parasoltech.com
# mcaD?7656


app = Flask(__name__)

# This is for local Development only .... !!
POSTGRES = {
    'user': 'postgres',
    'pw': 'password',
    'db': 'my_database',
    'host': 'localhost',
    'port': '5432',
}

app.config['DEBUG'] = True
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://%(user)s:\
%(pw)s@%(host)s:%(port)s/%(db)s' % POSTGRES
db.init_app(app)



@app.route('/')

def index():
    return render_template('index.html') 

@app.route('/about')
def about(): 
    return "This sisdfasdfklasndflk nadslk "

@app.route('/contact')
def contact(): 
    return 'sdfdsf'

@app.route('/test')
def someUrl():
    my_dict = {'title': 'Bayside', "colour" : "green"}
    return json.dumps(my_dict)

if __name__ == '__main__':
    app.run()


