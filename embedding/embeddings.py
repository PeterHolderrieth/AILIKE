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
import warnings
import pickle

#Make script deterministic:
torch.manual_seed

#Initialize model
model_name = "bert-base-uncased"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)


# Select pre-trained model
def generate_embedding(sentence: str, random_proj: bool = False):

    tokens = tokenizer(sentence, return_tensors="pt")
    outputs = model(**tokens)
    embeddings = outputs.last_hidden_state[0].mean(axis=0).flatten()
    if random_proj:
        embeddings = torch.matmul(embeddings,PROJ_MAT)
    return embeddings.tolist()

#Projection matrix if wanted:
RANDOM_PROJ = False
PROJ_DIM = 32
MODEL_DIM = len(generate_embedding(sentence="testtext", random_proj=False))
if RANDOM_PROJ:
    with open(f"proj_mat/rand_proj_mat_modeldim={MODEL_DIM}_embeddim={PROJ_DIM}.pkl", 'rb') as f:
        PROJ_MAT = pickle.load(f).transpose(1,0)

# python_server.py
app = Flask(__name__)

@app.route('/dimemb', methods=['POST'])
def get_embedding_dim():
    if RANDOM_PROJ:
        return jsonify({'dimemb': PROJ_DIM})
    else:
        return jsonify({'dimemb': MODEL_DIM})


@app.route('/embed', methods=['POST'])
def get_embedding():
    data = request.json
    embedding = generate_embedding(data['text'], RANDOM_PROJ)    
    return jsonify({'embedding': embedding})

if __name__ == '__main__':
    if RANDOM_PROJ:
        warnings.warn("You are computing embeddings ")
    app.run(host='0.0.0.0', port=7010)
