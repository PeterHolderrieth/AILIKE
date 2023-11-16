"""Script to explore various models. Not used in production but only for code development."""

from transformers import AutoTokenizer, AutoModel
import torch
import pandas as pd

DEVICE = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")


model_ckpt = "sentence-transformers/multi-qa-mpnet-base-dot-v1"
model_ckpt = "BAAI/bge-base-en-v1.5"
model_ckpt = "BAAI/bge-small-en-v1.5"
tokenizer = AutoTokenizer.from_pretrained(model_ckpt)
model = AutoModel.from_pretrained(model_ckpt)

model.to(DEVICE)

data_fpath = "/Users/peterholderrieth/Documents/code/AILIKE/data/tweet_emotions.csv"
df = pd.read_csv(data_fpath)
batch_size = 1000
n_batches = 30
documents = df["content"].sample(n_batches*batch_size).tolist()

# documents = [
#     "The Mediterranean diet emphasizes fish, olive oil, and vegetables, believed to reduce chronic diseases.",
#     "Photosynthesis in plants converts light energy into glucose and produces essential oxygen.",
#     "20th-century innovations, from radios to smartphones, centered on electronic advancements.",
#     "Rivers provide water, irrigation, and habitat for aquatic species, vital for ecosystems.",
#     "Apple’s conference call to discuss fourth fiscal quarter results and business updates is scheduled for Thursday, November 2, 2023 at 2:00 p.m. PT / 5:00 p.m. ET.",
#     "Shakespeare's works, like 'Hamlet' and 'A Midsummer Night's Dream,' endure in literature.",
#     "Goethe is a Germam poet. He is most famous for his work on Faust.",
#     "Amazon went almost bankrupt. Fortunately for the tech company, Jeff Bezos' confidence helped relax the stock market."
# ]

def cls_pooling(model_output):
    return model_output.last_hidden_state[:, 0]

def get_embeddings(text_list):
    encoded_input = tokenizer(
        text_list, padding=True, truncation=True, return_tensors="pt"
    )
    encoded_input = {k: v.to(DEVICE) for k, v in encoded_input.items()}
    model_output = model(**encoded_input)
    return cls_pooling(model_output)

# embeddings = []
# for batch_id in range(n_batches):
#     embeddings.append(get_embeddings(documents[batch_id*batch_size:(batch_id+1)*batch_size]))
# embeddings = torch.stack(embeddings)

queries = ["I am so happy.","Hair migration patterns of professors."]
query_embeddings = get_embeddings(queries)

print(query_embeddings.shape)
sys.exit("")
#sim_matrix = torch.matmul(embeddings,embeddings.transpose(1,0))
#sim_matrix = sim_matrix/(embeddings.norm(dim=1)[:,None]*embeddings.norm(dim=1)[None,:])

print("-----------------------------------")
print("-----------------------------------")
print("-----------------------------------")
print("-----------------------------------")
for idx in range(len(query_embeddings)):
    sim_row = torch.matmul(embeddings,query_embeddings[idx])
    sim_row[idx] = 0.0
    print("Query document: ", queries[idx])
    print("Most similar document: ", documents[torch.argmax(sim_row)])

# sim_matrix = ((embeddings[None,:,:]-embeddings[:,None,:])**2).sum(axis=2)
# print(sim_matrix)



# query_embedding = model.encode('How big is London')
# passage_embedding = model.encode(['London has 9,787,426 inhabitants at the 2011 census',
#                                   'London is known for its finacial district'])

# print("Similarity:", util.dot_score(query_embedding, passage_embedding))