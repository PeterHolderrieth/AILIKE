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
dataset = load_dataset('wikipedia', "20220301.en", split='train')

def process_dataitem_to_godb(data_item: dict, char_length: int):
    chop_length = min(len(data_item['text']),char_length)
    data_item['articleStart'] = data_item['text'][:(chop_length-3)]+"..."
    del data_item['text']
    return data_item

# python_server.py
app = Flask(__name__)

@app.route('/descriptor', methods=['POST'])
def get_descriptor():
    return jsonify({'desriptor': ['id','url','title','articleStart']})

@app.route('/dataitem', methods=['POST'])
def get_item():
    data = dataset[int(request.json['idx'])]
    charlength = int(request.json['char_length'])
    data = process_dataitem_to_godb(data, charlength)
    return jsonify({"dataelement": data})

@app.route('/dataitemfull', methods=['POST'])
def get_item_full_text():
    data = dataset[int(request.json['idx'])]
    return jsonify({"dataelement": data})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7011)
