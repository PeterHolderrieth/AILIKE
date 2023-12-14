"""Script that builds a python server that will be called from GoDB to fact-check statement."""

import openai
import pandas as pd
import datasets
from datasets import load_dataset
from typing import List
from flask import Flask, request, jsonify

openai.api_key = "{INSERT_YOUR_OPENAI_TOKEN}"

CONTEXT_SIZE = 2000
MODEL_NAME = "gpt-3.5-turbo"

dataset = load_dataset('wikipedia', "20220301.en")['train']



# python_server.py
app = Flask(__name__)

def get_full_wikipedia_pages_context(index_list: List[int], context_size: int = CONTEXT_SIZE):
  retrieved_data = dataset[index_list]
  len_per_article = int(context_size/len(index_list))-1
  context = ""
  for idx in range(len(index_list)):
    context += "\n---------\n"+retrieved_data["title"][idx] + ": \n\n"
    context += retrieved_data["text"][idx][:len_per_article] + "\n"

  return context

#user_question = "What is meaning of life? I am clueless. Can you help me?"
#print(get_query_suggestions(user_question))

def get_final_response_with_chatgpt(user_question: str, context: str):

  print("********************************************************")
  print("Gave GPT context and waiting for final response.")
  
  completion = openai.ChatCompletion.create(
    model = MODEL_NAME,#	gpt-4-1106-preview
    temperature = 0.8,
    max_tokens = 2000,
    messages = [
      {"role": "system", "content": "You are an expert in all subjects. Provide most helpful assistance."},
      {"role": "user", "content": context + "\nPlease use the wikipedia article above. Answer the question: " + user_question},
    ]
  )
  return_message = completion.choices[0].message

  return return_message

def get_fact_check_response_chatgpt(fact: str, context: str):

  print("********************************************************")
  print("Gave GPT context and waiting for final response.")
  
  completion = openai.ChatCompletion.create(
    model = MODEL_NAME,#	gpt-4-1106-preview
    temperature = 0.8,
    max_tokens = 2000,
    messages = [
      {"role": "system", "content": "You are an expert in all subjects. Provide most helpful assistance."},
      {"role": "user", "content": context + "\nDon't use our opinion but cite the artices above. I heard the following fact: "+fact +" . Is that true?"},
    ]
  )
  return_message = completion.choices[0].message

  return return_message

@app.route('/chatgptfactcheck', methods=['POST'])
def get_fact_check_chatgpt():
    
    data = request.json
    with_context = bool(data.get("with_context",1))
    if with_context:
      fact_check_context = get_full_wikipedia_pages_context(data['query_matches'])
    else:
       fact_check_context = ""
    print("Got fact check context: ", fact_check_context)

    return_message = get_fact_check_response_chatgpt(data['fact'], fact_check_context)['content']

    print("got return message: ", return_message)

    return jsonify({'Response': return_message})



# context = get_full_wikipedia_pages_context([17])
# print("Context: ")
# print(context)
# print()
# response = get_final_response_with_chatgpt("I believe Alain Connes was born in 1948 in Amboise, France. Is that correct?", context=context)
# print(response["content"])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7012)