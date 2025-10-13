# Filename: src/components/model_nlp_ner_tf.py

import tensorflow as tf
from transformers import DistilBertTokenizerFast, TFDistilBertForTokenClassification
from sklearn.model_selection import train_test_split
import numpy as np
import joblib
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.logger import *

import logging
logger = logging.getLogger(__name__)


# Synthetic BIO data (tokens and tags)
examples = [
    (["Delay", "in", "Shanghai", "due", "to", "storms"],  ["O", "O", "B-LOC", "O", "O", "B-EVENT"]),
    (["Alternate", "route", "to", "Mumbai", "advised"],    ["O", "O", "O", "B-LOC", "O"]),
    (["Flood", "events", "affect", "Delhi", "region"],     ["B-EVENT", "O", "O", "B-LOC", "O"]),
    (["Weather", "disrupts", "Beijing", "supply", "chain"],["B-EVENT", "O", "B-LOC", "O", "O", "O"]),
    (["Are", "there", "incidents", "in", "Shenzhen", "?"], ["O", "O", "B-EVENT", "O", "B-LOC", "O"]),
    (["Strike", "expected", "at", "Rotterdam", "port"],     ["B-EVENT", "O", "O", "B-LOC", "O"])
]

sentences = [s for s, t in examples]
tags      = [t for s, t in examples]
unique_tags = sorted(set(l for ts in tags for l in ts))
label2id  = {t: i for i, t in enumerate(unique_tags)}
id2label  = {i: t for t, i in label2id.items()}

# Build padded token/label arrays
max_len = max(len(s) for s in sentences)
tokenizer = DistilBertTokenizerFast.from_pretrained('distilbert-base-uncased')

def encode(sentences, labels, label2id, max_len):
    encodings = tokenizer(sentences, is_split_into_words=True, padding='max_length', truncation=True, max_length=max_len, return_tensors='tf')
    label_ids = []
    sample_weights = []
    for i, labs in enumerate(labels):
        ids = [label2id[l] for l in labs]
        padding_length = max_len - len(ids)
        ids += [0]*padding_length  # Use class 0 for padding
        weights = [1]*len(labs) + [0]*padding_length  # 1 for real, 0 for padding
        label_ids.append(ids)
        sample_weights.append(weights)
    encodings['labels'] = tf.convert_to_tensor(label_ids)
    encodings['sample_weights'] = tf.convert_to_tensor(sample_weights, dtype=tf.float32)
    return encodings


X_train, X_test, y_train, y_test = train_test_split(sentences, tags, test_size=0.2, random_state=42)
train_inputs = encode(X_train, y_train, label2id, max_len)
test_inputs  = encode(X_test, y_test, label2id, max_len)

# Model init
model = TFDistilBertForTokenClassification.from_pretrained(
    'distilbert-base-uncased',
    num_labels=len(label2id),
    id2label=id2label,
    label2id=label2id
)

# Model training
optimizer = tf.keras.optimizers.Adam(learning_rate=5e-5)
loss      = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
model.compile(optimizer=optimizer, loss=loss, metrics=['accuracy'])

logger.info("Starting NER model training.")
model.fit(
    {k: v for k, v in train_inputs.items() if k not in ['labels', 'sample_weights']},
    train_inputs['labels'],
    sample_weight=train_inputs['sample_weights'],
    epochs=8,
    batch_size=2
)
logger.info("Training complete.")

# Save model artifacts
out_dir = Path(__file__).resolve().parents[2] / "artifacts" / "models" / "nlp_ner"
out_dir.mkdir(parents=True, exist_ok=True)
model.save_pretrained(out_dir / "ner_model")
tokenizer.save_pretrained(out_dir / "ner_tokenizer")
joblib.dump(label2id, out_dir / "label2id.joblib")

logger.info(f"NER (TF) model, tokenizer, and label map saved to {out_dir}")

# --- Inference Demo ---
def predict_entities(sentence):
    tokens = sentence.split()
    encoding = tokenizer([tokens], is_split_into_words=True, return_tensors='tf', padding='max_length', truncation=True, max_length=max_len)
    outputs = model({k: v for k, v in encoding.items() if k != "labels"})
    logits = outputs.logits.numpy()[0]
    pred_ids = np.argmax(logits, axis=-1)

    tok_words = [w for (w, id) in zip(tokens, pred_ids[:len(tokens)])]
    tok_labels = [id2label[id] for id in pred_ids[:len(tokens)]]
    logger.info(f"Entities for: {sentence}")
    for w, l in zip(tok_words, tok_labels):
        if l != "O":
            logger.info(f"{w:15s} --> {l}")

# Tests
predict_entities("Flood in Mumbai caused delays")
predict_entities("Storm warning for Shanghai route")


def extract_entities(text: str) -> dict:
    import joblib
    import numpy as np
    from transformers import DistilBertTokenizerFast, TFDistilBertForTokenClassification
    from pathlib import Path

    model_dir = Path(__file__).resolve().parents[2] / "artifacts" / "models" / "nlp_ner"
    model = TFDistilBertForTokenClassification.from_pretrained(model_dir / "ner_model")
    tokenizer = DistilBertTokenizerFast.from_pretrained(model_dir / "ner_tokenizer")
    label2id = joblib.load(model_dir / "label2id.joblib")
    id2label = {i: t for t, i in label2id.items()}
    max_len = 32

    tokens = text.split()
    encoding = tokenizer([tokens], is_split_into_words=True, return_tensors='tf', padding='max_length', truncation=True, max_length=max_len)
    outputs = model({k: v for k, v in encoding.items() if k != "labels"})
    logits = outputs.logits.numpy()[0]
    pred_ids = np.argmax(logits, axis=-1)

    entities = {"location": [], "event": []}
    for w, id in zip(tokens, pred_ids[:len(tokens)]):
        label = id2label[id]
        if label == "B-LOC":
            entities["location"].append(w)
        elif label == "B-EVENT":
            entities["event"].append(w)
    return entities