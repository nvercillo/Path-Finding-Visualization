import logging
from flask import Flask, render_template, Response
import csv
import json 
import os 

app = Flask(__name__)
#app.config['TESTING'] = True
#app.config['DEBUG'] = True

@app.route('/')
def index():
    return render_template('index.html') 



# @app.route('/federato/data-pipeline')
# def federato():
    
    
#     text_files = sh.find(".", "-iname", "*.txt")

#     os.system("docker-compose -f ../extract-layer/docker-compose.test-prod.yml up --build")

#     return json.dumps(text_files)
#     # return "RUNNING PIPELINE"
    

@app.route('/path-finding-visualization')
def second_pages():
    return render_template('about_path_finding.html')


@app.route('/path-finding-visualization/demo')
def demo_template():
    return render_template('pathfinding.html')

@app.route('/portrait')
def get_image(pid):
    image_binary = read_image(pid)
    response = make_response(image_binary)
    response.headers.set('Content-Type', 'image/png')
    response.headers.set(
        'Content-Disposition', 'attachment', filename='%s.jpg' % pid)
    return response

@app.route('/getFlightData')
def about(): 

    filepath = 'openflights2/data/airports-extended.dat' 
    airportdata = {}
    airports = {}
    routes = {}

    f = open(filepath, "r", encoding="utf8")
    line = f.readline()
    # skip the first line 
    line = f.readline()
    while line:
        inds = []
        inexpres = False
        for i in range(0, len(line)):
            if line[i] == '"':
                if inexpres:
                    inexpres = False
                else:
                    inexpres = True
            if line[i] == ',' and not inexpres:
                inds.append(i)

        airports[line[inds[3]+2: inds[4]-1]] = {} 
        airports[line[inds[3]+2: inds[4]-1]]["country"] = line[inds[2]+2 : inds[3]-1]
        airports[line[inds[3]+2: inds[4]-1]]['coords'] = {}
        airports[line[inds[3]+2: inds[4]-1]]['coords']["latitude"] = float(line[inds[5]+1: inds[6]])
        airports[line[inds[3]+2: inds[4]-1]]['coords']["longitude"] = float(line[inds[6]+1: inds[7]])
        airports[line[inds[3]+2: inds[4]-1]]["name"] = line[inds[0]+2: inds[1]-1]
        line = f.readline()

    f.close()

    filepath = 'unavailibleAirports.txt'
    f = open(filepath, "r", encoding="utf8")
    line = f.readline()

    set = {'a'}
    while line:
        set.add(line[0:len(line)-1])
        line = f.readline()

    set.remove('a')
    f.close()

    filepath = 'openflights2/data/routes.dat'
    f = open(filepath, "r", encoding="utf8")
    line = f.readline()
    dict = {}
    while line:
        inds = []
        for i in range(0, len(line)):
            if line[i] == ',' :
                inds.append(i)
        if line[inds[1] +1 : inds[2]] not in set and line[inds[3] +1 : inds[4]] not in set:
            if line[inds[1] +1 : inds[2]] not in routes:
                routes[line[inds[1] +1 : inds[2]]] = []
            if line[inds[3] +1 : inds[4]] not in routes[line[inds[1] +1 : inds[2]]]:
                routes[line[inds[1] +1 : inds[2]]].append(line[inds[3] +1 : inds[4]])
        line = f.readline()

    f.close()
    airportdata['airports'] = airports
    airportdata['routes'] = routes
    return Response(json.dumps(airportdata), mimetype='application/json')
    return json.dumps(airportdata)

if __name__ == '__main__':
    print( "SDFSDFDSF")
    logging.basicConfig(filename='error.log',level=logging.DEBUG)
    app.run(port=5656)