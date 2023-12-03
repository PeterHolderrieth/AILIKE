"""
Script to get LLM to generate queries for GODB
"""
from flask import Flask, request, jsonify
import languagemodels as lm



app = Flask(__name__)

@app.route('/gen', methods=['POST'])
def get_embedding():
    data = request.json
    response = lm.chat(data['text'])  
    print("-----")
    print(response)
    print("-----")
    return jsonify({'response': response})

if __name__ == '__main__':
  app.run(host='0.0.0.0', port=7020)