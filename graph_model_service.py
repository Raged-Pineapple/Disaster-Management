# graph_model_service.py
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/insert', methods=['POST'])
def insert_data():
    data = request.get_json()
    print("Received data for graph insertion:", data)
    # Later we'll add Neo4j insertion here
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(port=5001)
