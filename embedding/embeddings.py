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
DEVICE = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")

#As a model, we currently use the BAAI General Embedding (BGE) small model:
#These models are trained as follows:
# 1. Pre-training: LLMs based on masked auto-encoders with a one-layer decoder
# 2. Fine-tuning: contrastive learning fine-tuning for negative/positive similarity examples
# Resources:
# RetroMAE (pre-trained model): https://arxiv.org/pdf/2205.12035.pdf
# FlagEmbedding (fine-tuned model): https://github.com/FlagOpen/FlagEmbedding
model_ckpt = "BAAI/bge-small-en-v1.5"
#model_ckpt = "BAAI/bge-base-en-v1.5" #Larger BGE model
#model_ckpt = "sentence-transformers/multi-qa-mpnet-base-dot-v1" #MPNET from 2020

#Initialize model
tokenizer = AutoTokenizer.from_pretrained(model_ckpt)
model = AutoModel.from_pretrained(model_ckpt)


def cls_pooling(model_output: torch.Tensor):
    return model_output.last_hidden_state[:, 0]

# Select pre-trained model
def generate_embedding(sentence: str, random_proj: bool = False):

    tokens = tokenizer(sentence, padding=True, truncation=True, return_tensors="pt")
    
    encoded_input = {k: v.to(DEVICE) for k, v in tokens.items()}

    outputs = model(**tokens)
    embeddings = cls_pooling(outputs)
    if random_proj:
        embeddings = torch.matmul(embeddings,PROJ_MAT)

    return embeddings.squeeze().tolist()

#Projection matrix if wanted:
RANDOM_PROJ = True
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
        warnings.warn("You are projecting the embeddings with a random projection.")
    app.run(host='0.0.0.0', port=7010)
