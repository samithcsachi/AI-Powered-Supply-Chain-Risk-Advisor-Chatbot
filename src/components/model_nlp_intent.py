import pandas as pd
import numpy as np
import tensorflow as tf
from transformers import DistilBertTokenizer, TFDistilBertForSequenceClassification
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import joblib
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.logger import *

import logging
logger = logging.getLogger(__name__)

def generate_synthetic_data():
    """Generate synthetic training data for intent classification."""
    data = {
        'text': [
            # Risk Check Intent
            "What's the risk for Mumbai shipments?",
            "Any delays expected for Shanghai routes?",
            "Is there disruption risk for my order?",
            "Check risk status for Delhi delivery",
            "Are there any supply chain issues?",
            
            # Weather Alert Intent
            "Any weather alerts today?",
            "What's the weather situation in Beijing?",
            "Are there storms affecting deliveries?",
            "Weather conditions for logistics?",
            "Any severe weather warnings?",
            
            # Mitigation Help Intent
            "What should I do about delays?",
            "How to avoid supply chain risks?",
            "Suggest alternative routes",
            "What are my options for rerouting?",
            "Help me mitigate delivery issues",
            
            # General Query Intent
            "Hello, how can you help?",
            "What can this system do?",
            "I need information about logistics",
            "Tell me about your capabilities",
            "How does this chatbot work?"
        ],
        'intent': [
            'risk_check', 'risk_check', 'risk_check', 'risk_check', 'risk_check',
            'weather_alert', 'weather_alert', 'weather_alert', 'weather_alert', 'weather_alert',
            'mitigation_help', 'mitigation_help', 'mitigation_help', 'mitigation_help', 'mitigation_help',
            'general_query', 'general_query', 'general_query', 'general_query', 'general_query'
        ]
    }
    return pd.DataFrame(data)

def main():
   
    df = generate_synthetic_data()
    

    label_encoder = LabelEncoder()
    df['label'] = label_encoder.fit_transform(df['intent'])
    
   
    X_train, X_test, y_train, y_test = train_test_split(
        df['text'], df['label'], test_size=0.2, random_state=42, stratify=df['label']
    )
    
 
    tokenizer = DistilBertTokenizer.from_pretrained('distilbert-base-uncased')
    model = TFDistilBertForSequenceClassification.from_pretrained(
        'distilbert-base-uncased', 
        num_labels=len(label_encoder.classes_)
    )
    
 
    train_encodings = tokenizer(list(X_train), truncation=True, padding=True, max_length=128, return_tensors='tf')
    test_encodings = tokenizer(list(X_test), truncation=True, padding=True, max_length=128, return_tensors='tf')
    
 
    train_dataset = tf.data.Dataset.from_tensor_slices((
        dict(train_encodings),
        y_train.values
    )).batch(8)
    
    
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=5e-5),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        metrics=['accuracy']
    )
    
    # Train model
    model.fit(train_dataset, epochs=3)
    
    # Save model and tokenizer
    model_dir = Path(__file__).resolve().parents[2] / "artifacts" / "models" / "nlp_intent"
    model_dir.mkdir(parents=True, exist_ok=True)
    
    model.save_pretrained(model_dir / "intent_model")
    tokenizer.save_pretrained(model_dir / "intent_tokenizer")
    joblib.dump(label_encoder, model_dir / "label_encoder.joblib")
    
    logger.info(f"Intent classification model saved to {model_dir}")
    
  
    test_queries = [
        "Is there risk for my Beijing shipment?",
        "Any weather problems today?",
        "What should I do about delays?"
    ]
    
    for query in test_queries:
        inputs = tokenizer(query, return_tensors='tf', truncation=True, padding=True, max_length=128)
        outputs = model(inputs)
        predicted_class = tf.argmax(outputs.logits, axis=1).numpy()[0]
        intent = label_encoder.inverse_transform([predicted_class])[0]
        confidence = tf.nn.softmax(outputs.logits)[0][predicted_class].numpy()
        
        logger.info(f"Query: '{query}' -> Intent: {intent} (Confidence: {confidence:.3f})")


def predict_intent(text: str) -> dict:
    """
    Load trained model, tokenizer, and label encoder, then predict intent for given text.
    """
    import joblib
    from transformers import DistilBertTokenizer, TFDistilBertForSequenceClassification
    import tensorflow as tf
    from pathlib import Path

    model_dir = Path(__file__).resolve().parents[2] / "artifacts" / "models" / "nlp_intent"
    model = TFDistilBertForSequenceClassification.from_pretrained(model_dir / "intent_model")
    tokenizer = DistilBertTokenizer.from_pretrained(model_dir / "intent_tokenizer")
    label_encoder = joblib.load(model_dir / "label_encoder.joblib")

    inputs = tokenizer(text, return_tensors="tf", truncation=True, padding=True, max_length=128)
    outputs = model(inputs)
    predicted_class = tf.argmax(outputs.logits, axis=1).numpy()[0]
    intent = label_encoder.inverse_transform([predicted_class])[0]
    confidence = float(tf.nn.softmax(outputs.logits)[0][predicted_class].numpy())
    return {"intent": intent, "confidence": confidence}


if __name__ == "__main__":
    main()
