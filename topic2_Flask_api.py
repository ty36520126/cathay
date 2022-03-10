import pymongo
import flask
import json
from flask import Flask, request, jsonify
from flask_cors import CORS

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['cathay']
collection = db['rent591']


app = Flask(__name__)
CORS(app)

@app.route('/newtaipei_man', methods=['GET'])
def filter1(): # 男生可承租  且  位於新北 的租屋物件
    myquery1 = { "縣市": "新北市", "性別要求": { "$ne": "限女生租住" } }
    filter1 = []
    for x in collection.find(myquery1, {"_id":0}):
        filter1.append(x)
    f1 = list(filter1)
    return jsonify(f1)


@app.route('/taipei_wu_f', methods=['GET'])
def filter2(): #【 臺北 】【 屋主為女性 】【 姓氏 為吳 】
    myquery2 = { "縣市": "台北市", "出租者": { "$regex": "太太|小姐" }, "出租者": { "$regex": "^吳" }  }
    filter2 = []
    for x in collection.find(myquery2, {"_id":0}):
        filter2.append(x)
    f2 = list(filter2)
    return jsonify(f2)

@app.route('/phone', methods=['POST'])
def filter3(): #以 【 聯絡電話 】 查詢租屋物件
    inp = request.get_json()
    phone = inp['聯絡電話']
    myquery3 = { "聯絡電話": phone }
    filter3 = []
    for x in collection.find(myquery3, {"_id":0}):
        filter3.append(x)
    f3 = list(filter3)
    return jsonify(f3)

@app.route('/not_host', methods=['GET'])
def filter4(): #所有 【 非屋主自行刊登 】 的租屋物件
    myquery4 = { "出租者身分": {"$ne" : "屋主"} }
    filter4 = []
    for x in collection.find(myquery4, {"_id":0}):
        filter4.append(x)
    f4 = list(filter4)
    return jsonify(f4)


app.run()