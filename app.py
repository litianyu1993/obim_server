from flask import Flask, request
from flask_cors import CORS, cross_origin
import pymongo
from flask import jsonify, Response
import numpy as np
import json
from io import StringIO
import copy
import sys
import uuid

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)


@app.route('/', methods=['POST'])
@cross_origin()
def send_event():
    uri = "mongodb://tianyumongo:VvzxetVYlugFvqcvGeQFsY0roLS4Wy90VzwdPVf10jqnhds4ND8zoQrSOBbZvlWewJmFaff9uDxC73ZXfOovXw==@tianyumongo.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@tianyumongo@"
    client = pymongo.MongoClient(uri)

    mydb = client["test-database"]

    list_of_dict = json.load(StringIO(request.data.decode('utf-8')))

    if len(list_of_dict['data']) == 0:
        return 'no data'
    else:
        session_id = uuid.uuid4()
        mycol = mydb[list_of_dict['source']]
        for dict in list_of_dict['data']:
            # print(dict.dtype)
            dict['sessionID'] = session_id
            mycol.insert_one(copy.deepcopy(dict))
        return 'success'


@app.route('/test', methods=['GET'])
def test_retrieve():
    uri = "mongodb://tianyumongo:VvzxetVYlugFvqcvGeQFsY0roLS4Wy90VzwdPVf10jqnhds4ND8zoQrSOBbZvlWewJmFaff9uDxC73ZXfOovXw==@tianyumongo.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@tianyumongo@"
    client = pymongo.MongoClient(uri)

    mydb = client["test-database"]
    mycol = mydb["office_data"]
    event_list = mycol.find({})
    new_event_list = []
    for event in event_list:
        new_event = {}
        for key in event:
            if key == '_id':
                continue
            else:
                if key == 'DOM':
                    tmp = json.load(StringIO(event[key]))

                else:
                    tmp = event[key]
                new_event[key] = tmp

        new_event_list.append(new_event)

    return "<pre>{}</pre>".format(json.dumps(new_event_list, indent=4))

@app.route('/action', methods=['POST'])
@cross_origin()
def send_action():
    uri = "mongodb://tianyumongo:VvzxetVYlugFvqcvGeQFsY0roLS4Wy90VzwdPVf10jqnhds4ND8zoQrSOBbZvlWewJmFaff9uDxC73ZXfOovXw==@tianyumongo.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@tianyumongo@"
    client = pymongo.MongoClient(uri)

    mydb = client["test-database"]

    list_of_dict = json.load(StringIO(request.data.decode('utf-8')))

    if len(list_of_dict['data']) == 0:
        return 'no data'
    else:
        session_id = uuid.uuid4()
        mycol = mydb[list_of_dict['source']]
        for dict in list_of_dict['data']:
            # print(dict.dtype)
            dict['sessionID'] = session_id
            mycol.insert_one(copy.deepcopy(dict))
        return 'success'

if __name__ == "__main__":
    app.run(host = '0.0.0.0')
