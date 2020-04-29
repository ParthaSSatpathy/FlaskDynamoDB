from flask import Flask, request, json
app = Flask(__name__)

from dynamo import Dynamo
region_name = 'us-east-1'#Update your region here

@app.route('/')
def hello():
    return "OK"

@app.route('/createtables', methods=['GET'])
def createtables():
    dyn = Dynamo(region_name)
    message = dyn.create_tables()
    return str(message)

@app.route('/uploaddata', methods=['GET'])
def uploaddata():
    dyn = Dynamo(region_name)
    message = dyn.upload_data()
    return str(message)

if __name__ == '__main__':
    print('Starting Server')
    app.run('0.0.0.0',port=5000)#Make sure to make the 5000 port available for TCP in Security Group