---
dataset_info:
  features:
  - name: _id
    dtype: string
  - name: title
    dtype: string
  - name: text
    dtype: string
  - name: text-embedding-3-large-1536-embedding
    sequence: float64
  splits:
  - name: train
    num_bytes: 12679725776
    num_examples: 1000000
  download_size: 9551862565
  dataset_size: 12679725776
configs:
- config_name: default
  data_files:
  - split: train
    path: data/train-*
license: mit
task_categories:
- feature-extraction
language:
- en
size_categories:
- 1M<n<10M
---


1M OpenAI Embeddings: text-embedding-3-large 1536 dimensions

- Created: February 2024. 
- Text used for Embedding: title (string) + text (string)
- Embedding Model: OpenAI text-embedding-3-large
- This dataset was generated from the first 1M entries of https://huggingface.co/datasets/BeIR/dbpedia-entity, extracted by @KShivendu_ [here](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M)