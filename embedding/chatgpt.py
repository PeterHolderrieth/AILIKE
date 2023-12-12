import openai
import pandas as pd
openai.api_key = "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"

def get_query_suggestions(user_question: str):
  print("********************************************************")
  print("Received user question. Getting reasonable sub-questions.")
  
  completion = openai.ChatCompletion.create(
    model = "gpt-4",
    temperature = 0.8,
    max_tokens = 2000,
    messages = [
      {"role": "system", "content": "You are an expert in all subjects. Provide most helpful assistance."},
      {"role": "user", "content": f"I am trying to answer the user's question: {user_question}. Can you give me a list of 10 sub-questions to ask and give preliminary answers? Please separate them by a linebreak in the format (Title, Suggested content). Don't add any numbers."},
    ]
  )
  return_message = completion.choices[0].message
  suggestions = [el.replace('"', '') for el in return_message["content"].split("\n")]
  suggestions = [el.lstrip("(").rstrip(")").split(",") for el in suggestions]
  suggestions = pd.Series(suggestions)
  suggestions = suggestions[suggestions.apply(lambda x: len(x)) == 2]
  suggestions = suggestions.apply(lambda x: [x[0].strip(" "), x[1].strip(" ")]).tolist()

  print("Done getting reasonable Wikipedia articles to look up.")
  print("********************************************************")  
  print()
  return suggestions

def get_full_wikipedia_pages():

# user_question = "What is meaning of life? I am clueless. Can you help me?"
# print(get_wikipedia_pages(user_question))