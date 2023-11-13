"""
Script to create LLM embeddings of text
"""
from pathlib import Path
from typing import List
import datasets
from datasets import load_dataset, interleave_datasets
import torch
from transformers import GPT2LMHeadModel, PreTrainedTokenizer, AutoTokenizer, Trainer, TrainingArguments, AutoConfig
import math
import datetime
from pathlib import Path
import datasets
from datasets import load_dataset, interleave_datasets
import torch
import transformers
from transformers import PreTrainedTokenizer
from transformers import GPT2LMHeadModel, PreTrainedTokenizer, AutoTokenizer, Trainer, TrainingArguments, AutoConfig
import random
import math
import os
from flask import Flask, request, jsonify
from transformers import AutoTokenizer, AutoModel
import numpy as np


#Initialize model
model_name = "bert-base-uncased"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)

 
# Select pre-trained model
def generate_embedding(sentence: str):

    tokens = tokenizer(sentence, return_tensors="pt")
    outputs = model(**tokens)
    embeddings = outputs.last_hidden_state[0].mean(axis=0).flatten()
    return embeddings.tolist()

# python_server.py
app = Flask(__name__)

@app.route('/dimemb', methods=['POST'])
def get_embedding_dim():
    model_dim = len(generate_embedding(sentence="testtext"))  
    return jsonify({'dimemb': model_dim})


@app.route('/embed', methods=['POST'])
def get_embedding():
    data = request.json
    embedding = generate_embedding(data['text'])    
    return jsonify({'embedding': embedding})

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=7010)
