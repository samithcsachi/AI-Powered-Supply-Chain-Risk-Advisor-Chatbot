import tensorflow as tf
from transformers import DistilBertTokenizerFast, TFDistilBertForTokenClassification, pipeline
from sklearn.model_selection import train_test_split
import numpy as np
import joblib
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.logger import *

import logging
logger = logging.getLogger(__name__)

EPOCHS = 30
BATCH_SIZE = 8
LEARNING_RATE = 5e-5
VALIDATION_SPLIT = 0.15
PATIENCE = 3

try:
    from tensorflow_addons.optimizers import AdamW
    optimizer = AdamW(learning_rate=LEARNING_RATE, weight_decay=1e-2)
except ImportError:
    optimizer = tf.keras.optimizers.Adam(learning_rate=LEARNING_RATE)



examples = [
    (["Delay", "in", "Shanghai", "due", "to", "storms"],  ["O", "O", "B-LOC", "O", "O", "B-EVENT"]),
    (["Any", "delay", "in", "vessel", "from", "USA", "to", "UAE", "?"], ["O", "O", "O", "O", "O", "B-LOC", "O", "B-LOC", "O"]),
    (["Cargo", "stuck", "at", "UAE", "port"], ["O", "O", "O", "B-LOC", "O"]),
    (["Weather", "alert", "for", "USA"], ["O", "O", "O", "B-LOC"]),
    (["Flood", "risk", "in", "Mumbai"], ["O", "O", "O", "B-LOC"]),
    (["Port", "closure", "Middle", "East"], ["O", "O", "B-LOC", "I-LOC"]),
    (["Is", "cargo", "delayed", "from", "USA", "to", "India", "?"], ["O", "O", "O", "O", "B-LOC", "O", "B-LOC", "O"]),
    (["Weather", "problems", "expected", "in", "USA"], ["O", "O", "O", "O", "B-LOC"]),
    (["Port", "strike", "at", "Singapore"], ["O", "O", "O", "B-LOC"]),
    (["Typhoon", "in", "Japan"], ["B-EVENT", "O", "B-LOC"]),
    (["Reroute", "shipments", "from", "Los", "Angeles"], ["O", "O", "O", "B-LOC", "I-LOC"]),
    (["Supply", "disruption", "Middle", "East"], ["O", "O", "B-LOC", "I-LOC"]),
    (["Severe", "fog", "in", "United", "Arab", "Emirates"], ["O", "O", "O", "B-LOC", "I-LOC", "I-LOC"]),
    (["Are", "shipments", "to", "Brazil", "affected", "by", "strike", "?"], ["O", "O", "O", "B-LOC", "O", "O", "B-EVENT", "O"]),
    (["Is", "Paris", "airport", "open", "after", "floods", "?"], ["O", "B-LOC", "O", "O", "O", "B-EVENT", "O"]),
    (["Delay", "reported", "in", "Berlin"], ["O", "O", "O", "B-LOC"]),
    (["Export", "hold", "at", "Los", "Angeles"], ["O", "O", "O", "B-LOC", "I-LOC"]),
    (["Typhoon", "warning", "for", "Japan"], ["B-EVENT", "O", "O", "B-LOC"]),
    (["Reroute", "cargo", "to", "Singapore"], ["O", "O", "O", "B-LOC"]),
    (["Is", "there", "labor", "strike", "in", "Canada", "?"], ["O", "O", "O", "B-EVENT", "O", "B-LOC", "O"]),
    (["Storm", "impact", "on", "United", "Kingdom"], ["B-EVENT", "O", "O", "B-LOC", "I-LOC"]),
    (["Supply", "disruption", "Italy"], ["O", "O", "B-LOC"]),
    (["Any", "hold-up", "in", "Dubai", "?",], ["O", "O", "O", "B-LOC", "O"]),
    (["Cargo", "delay", "at", "Rotterdam", "port"], ["O", "O", "O", "B-LOC", "O"]),
    (["Flood", "disrupts", "service", "in", "Turkey"], ["B-EVENT", "O", "O", "O", "B-LOC"]),
    (["Severe", "thunderstorm", "in", "New", "York", "City"], ["O", "B-EVENT", "O", "B-LOC", "I-LOC", "I-LOC"]),
    (["Is", "Shanghai", "port", "closed", "for", "holiday", "?"], ["O", "B-LOC", "O", "O", "O", "O", "O"]),
    (["France", "logistics", "strike"], ["B-LOC", "O", "B-EVENT"]),
    (["Export", "shipment", "to", "Spain", "delayed"], ["O", "O", "O", "B-LOC", "O"]),
    (["Cargo", "rerouted", "from", "Colombo", "to", "Sydney"], ["O", "O", "O", "B-LOC", "O", "B-LOC"]),
    (["Vessel", "from", "India", "held", "by", "customs"], ["O", "O", "B-LOC", "O", "O", "O"]),
    (["Is", "Singapore", "affected", "by", "monsoon", "season", "?"], ["O", "B-LOC", "O", "O", "B-EVENT", "I-EVENT", "O"]),
    (["Disruption", "in", "United", "Arab", "Emirates", "due", "to", "strike"], ["O", "O", "B-LOC", "I-LOC", "I-LOC", "O", "O", "B-EVENT"]),
    (["How", "long", "is", "the", "delay", "in", "Mexico", "?"], ["O", "O", "O", "O", "O", "O", "B-LOC", "O"]),
    (["Flood", "risk", "in", "Gujarat"], ["B-EVENT", "O", "O", "B-LOC"]),
    (["Severe", "weather", "disrupts", "Melbourne", "port"], ["B-EVENT", "O", "O", "B-LOC", "O"]),
    (["Export", "stopped", "from", "Jakarta", "because", "of", "strike"], ["O", "O", "O", "B-LOC", "O", "O", "B-EVENT"]),
    (["Storm", "warning", "for", "Delhi"], ["B-EVENT", "O", "O", "B-LOC"]),
    (["Any", "delay", "from", "United", "States", "to", "United", "Kingdom", "?"], ["O", "O", "O", "B-LOC", "I-LOC", "O", "B-LOC", "I-LOC", "O"]),
    (["Cargo", "stuck", "at", "Sao", "Paulo"], ["O", "O", "O", "B-LOC", "I-LOC"]),
    (["Shipping", "interruption", "in", "Cairo"], ["O", "O", "O", "B-LOC"]),
    (["Typhoon", "delays", "cargo", "to", "Hong", "Kong"], ["B-EVENT", "O", "O", "O", "B-LOC", "I-LOC"]),
    (["No", "disruption", "in", "Berlin"], ["O", "O", "O", "B-LOC"]),
    (["Port", "closure", "for", "Christmas", "in", "Canada"], ["O", "O", "O", "O", "O", "B-LOC"]),
    (["Is", "there", "a", "strike", "in", "Melbourne", "?"], ["O", "O", "O", "B-EVENT", "O", "B-LOC", "O"]),
    (["Shipment", "delayed", "in", "Mexico", "City"], ["O", "O", "O", "B-LOC", "I-LOC"]),
    (["Are", "vessels", "from", "Copenhagen", "blocked", "?"], ["O", "O", "O", "B-LOC", "O", "O"]),
    (["Heavy", "rains", "in", "Manila"], ["O", "B-EVENT", "O", "B-LOC"]),
    (["Strike", "at", "Johannesburg", "port"], ["B-EVENT", "O", "B-LOC", "O"]),
    (["Is", "the", "route", "from", "Italy", "to", "Brazil", "safe", "?"], ["O", "O", "O", "O", "B-LOC", "O", "B-LOC", "O", "O"]),
    (["Container", "stuck", "at", "Antwerp"], ["O", "O", "O", "B-LOC"]),
    (["Any", "blockade", "in", "Pakistan", "?"], ["O", "B-EVENT", "O", "B-LOC", "O"]),
    (["Flood", "alerts", "for", "Vietnam"], ["B-EVENT", "O", "O", "B-LOC"]),
    (["Are", "planes", "to", "Madrid", "canceled", "?"], ["O", "O", "O", "B-LOC", "O", "O"]),
    (["Shipments", "from", "Morocco", "are", "late"], ["O", "O", "B-LOC", "O", "O"]),
    (["Earthquake", "in", "Indonesia", "affecting", "deliveries"], ["B-EVENT", "O", "B-LOC", "O", "O"]),
    (["Rail", "disruption", "in", "Melbourne"], ["O", "B-EVENT", "O", "B-LOC"]),
    (["Any", "closure", "at", "Rotterdam", "port", "?"], ["O", "B-EVENT", "O", "B-LOC", "O", "O"]),
    (["Landslide", "blocks", "road", "to", "Lima"], ["B-EVENT", "O", "O", "O", "B-LOC"]),
    (["Flights", "to", "Bangkok", "suspended"], ["O", "O", "B-LOC", "O"]),
    (["Typhoon", "threat", "for", "Taipei"], ["B-EVENT", "O", "O", "B-LOC"]),
    (["Is", "Melbourne", "port", "operational", "today", "?"], ["O", "B-LOC", "O", "O", "O", "O"]),
    (["Japan", "export", "ban"], ["B-LOC", "O", "B-EVENT"]),
    (["Closure", "in", "Buenos", "Aires"], ["B-EVENT", "O", "B-LOC", "I-LOC"]),
    (["Truck", "strike", "delaying", "goods", "from", "Poland"], ["O", "B-EVENT", "O", "O", "O", "B-LOC"]),
    (["Shanghai", "flood", "disrupts", "cargo"], ["B-LOC", "B-EVENT", "O", "O"]),
    (["Supply", "held", "in", "Turkey", "because", "of", "strike"], ["O", "O", "O", "B-LOC", "O", "O", "B-EVENT"]),
    (["Port", "congestion", "in", "Los", "Angeles"], ["O", "B-EVENT", "O", "B-LOC", "I-LOC"]),
    (["Storm", "approaching", "Cape", "Town"], ["B-EVENT", "O", "B-LOC", "I-LOC"]),
    (["Bad", "weather", "New", "York"], ["O", "B-EVENT", "B-LOC", "I-LOC"]),
    (["Zambia", "roads", "closed", "due", "to", "flood"], ["B-LOC", "O", "O", "O", "O", "B-EVENT"]),
    (["Strike", "in", "Athens", "delays", "supply"], ["B-EVENT", "O", "B-LOC", "O", "O"]),
    (["Transport", "problem", "in", "Perth"], ["O", "B-EVENT", "O", "B-LOC"]),
    (["Typhoon", "interrupts", "shipments", "to", "Hong", "Kong"], ["B-EVENT", "O", "O", "O", "B-LOC", "I-LOC"]),
    (["Avalanche", "blocks", "Italian", "border"], ["B-EVENT", "O", "B-LOC", "O"]),
    
]


sentences = [s for s, t in examples]
tags      = [t for s, t in examples]
unique_tags = sorted(set(l for ts in tags for l in ts))
label2id  = {t: i for i, t in enumerate(unique_tags)}
id2label  = {i: t for t, i in label2id.items()}
max_len = max(len(s) for s in sentences)
tokenizer = DistilBertTokenizerFast.from_pretrained('distilbert-base-uncased')

def encode(sentences, labels, label2id, max_len):
    encodings = tokenizer(sentences, is_split_into_words=True, padding='max_length', truncation=True, max_length=max_len, return_tensors='tf')
    label_ids = []
    sample_weights = []
    for i, labs in enumerate(labels):
        ids = [label2id[l] for l in labs]
        padding_length = max_len - len(ids)
        ids += [0]*padding_length  
        weights = [1]*len(labs) + [0]*padding_length 
        label_ids.append(ids)
        sample_weights.append(weights)
    encodings['labels'] = tf.convert_to_tensor(label_ids)
    encodings['sample_weights'] = tf.convert_to_tensor(sample_weights, dtype=tf.float32)
    return encodings

def train_ner_model():
    X_train, X_val, y_train, y_val = train_test_split(sentences, tags, test_size=VALIDATION_SPLIT, random_state=42)
    train_inputs = encode(X_train, y_train, label2id, max_len)
    val_inputs = encode(X_val, y_val, label2id, max_len)


    model = TFDistilBertForTokenClassification.from_pretrained(
        'distilbert-base-uncased',
        num_labels=len(label2id),
        id2label=id2label,
        label2id=label2id
    )
    loss = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
    model.compile(optimizer=optimizer, loss=loss, metrics=['accuracy'],weighted_metrics=['accuracy'])


    callback = tf.keras.callbacks.EarlyStopping(
        monitor='val_loss',
        patience=PATIENCE,
        restore_best_weights=True
    )

    logger.info("Starting NER model training (tuned).")


    history = model.fit(
        {k: v for k, v in train_inputs.items() if k not in ['labels', 'sample_weights']},
        train_inputs['labels'],
        sample_weight=train_inputs['sample_weights'],
        epochs=EPOCHS,
        batch_size=BATCH_SIZE,
        validation_data=(
            {k: v for k, v in val_inputs.items() if k not in ['labels', 'sample_weights']},
            val_inputs['labels'],
            val_inputs['sample_weights']
        ),
        callbacks=[callback]
    )

    logger.info("Training complete.")
    logger.info(f"Best validation accuracy: {max(history.history['val_accuracy'])}")

    out_dir = Path(__file__).resolve().parents[2] / "artifacts" / "models" / "nlp_ner"
    out_dir.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(out_dir / "ner_model")
    tokenizer.save_pretrained(out_dir / "ner_tokenizer")
    joblib.dump(label2id, out_dir / "label2id.joblib")
    logger.info(f"NER (TF) model, tokenizer, and label map saved to {out_dir}")


def extract_entities_pipeline(text: str) -> dict:
    model_dir = Path(__file__).resolve().parents[2] / "artifacts" / "models" / "nlp_ner"
    custom_model = TFDistilBertForTokenClassification.from_pretrained(model_dir / "ner_model")
    custom_tokenizer = DistilBertTokenizerFast.from_pretrained(model_dir / "ner_tokenizer")
    label2id = joblib.load(model_dir / "label2id.joblib")
    id2label = {i: t for t, i in label2id.items()}
    max_len = 32
    tokens = text.split()
    encoding = custom_tokenizer([tokens], is_split_into_words=True, return_tensors='tf', padding='max_length', truncation=True, max_length=max_len)
    outputs = custom_model({k: v for k, v in encoding.items() if k != "labels"})
    logits = outputs.logits.numpy()[0]
    pred_ids = np.argmax(logits, axis=-1)
    custom_entities = {"location": [], "event": []}
    current_loc, current_evt = [], []
    for w, id in zip(tokens, pred_ids[:len(tokens)]):
        label = id2label[id]
        if label == "B-LOC":
            if current_loc:
                custom_entities["location"].append(" ".join(current_loc))
                current_loc = []
            current_loc = [w]
        elif label == "I-LOC" and current_loc:
            current_loc.append(w)
        else:
            if current_loc:
                custom_entities["location"].append(" ".join(current_loc))
                current_loc = []
        if label == "B-EVENT":
            if current_evt:
                custom_entities["event"].append(" ".join(current_evt))
                current_evt = []
            current_evt = [w]
        elif label == "I-EVENT" and current_evt:
            current_evt.append(w)
        else:
            if current_evt:
                custom_entities["event"].append(" ".join(current_evt))
                current_evt = []
    if current_loc:
        custom_entities["location"].append(" ".join(current_loc))
    if current_evt:
        custom_entities["event"].append(" ".join(current_evt))

    hf_ner = pipeline("ner", grouped_entities=True, model="dbmdz/bert-large-cased-finetuned-conll03-english")
    hf_results = hf_ner(text)
    hf_locations = [ent['word'] for ent in hf_results if ent['entity_group'] == "LOC"]

    all_locations = set(custom_entities["location"]) | set(hf_locations)
    all_events = custom_entities["event"]
    return {"location": list(all_locations), "event": all_events}


if __name__ == "__main__":
    train_ner_model()