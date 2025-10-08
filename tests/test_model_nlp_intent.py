import pytest
import numpy as np
import tensorflow as tf
from transformers import DistilBertTokenizer, TFDistilBertForSequenceClassification
import joblib
from pathlib import Path
import sys

# Add src to path for imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

class TestNLPIntentModel:
    """Test suite for NLP Intent Classification model."""
    
    @classmethod
    def setup_class(cls):
        """Load model artifacts once for all tests."""
        cls.model_dir = Path(__file__).resolve().parents[1] / "artifacts" / "models" / "nlp_intent"
        
        # Check if model exists
        if not (cls.model_dir / "intent_model").exists():
            pytest.skip("Intent model not found. Run model_nlp_intent.py first.")
        
        # Load model components
        cls.model = TFDistilBertForSequenceClassification.from_pretrained(cls.model_dir / "intent_model")
        cls.tokenizer = DistilBertTokenizer.from_pretrained(cls.model_dir / "intent_tokenizer")
        cls.label_encoder = joblib.load(cls.model_dir / "label_encoder.joblib")
    
    def test_model_loading(self):
        """Test that model components load successfully."""
        assert self.model is not None
        assert self.tokenizer is not None
        assert self.label_encoder is not None
        assert hasattr(self.model, 'predict')
    
    def test_tokenizer_functionality(self):
        """Test tokenizer works correctly."""
        sample_text = "What's the risk for Mumbai shipments?"
        inputs = self.tokenizer(sample_text, return_tensors='tf', truncation=True, padding=True, max_length=128)
        
        assert 'input_ids' in inputs
        assert 'attention_mask' in inputs
        assert inputs['input_ids'].shape[1] <= 128
    
    def test_intent_prediction_risk_check(self):
        """Test prediction for risk_check intent."""
        queries = [
            "What's the risk for Mumbai shipments?",
            "Any delays expected for Shanghai routes?",
            "Check risk status for Delhi delivery"
        ]
        
        for query in queries:
            inputs = self.tokenizer(query, return_tensors='tf', truncation=True, padding=True, max_length=128)
            outputs = self.model(inputs)
            predicted_class = tf.argmax(outputs.logits, axis=1).numpy()[0]
            intent = self.label_encoder.inverse_transform([predicted_class])[0]
            confidence = tf.nn.softmax(outputs.logits)[0][predicted_class].numpy()
            
            # Should predict risk_check with reasonable confidence
            assert intent == 'risk_check', f"Expected 'risk_check', got '{intent}' for query: {query}"
            assert confidence > 0.2, f"Low confidence ({confidence:.3f}) for query: {query}"
    
    def test_intent_prediction_weather_alert(self):
        """Test prediction for weather_alert intent."""
        queries = [
            "Any weather alerts today?",
            "What's the weather situation in Beijing?",
            "Are there storms affecting deliveries?"
        ]
        
        for query in queries:
            inputs = self.tokenizer(query, return_tensors='tf', truncation=True, padding=True, max_length=128)
            outputs = self.model(inputs)
            predicted_class = tf.argmax(outputs.logits, axis=1).numpy()[0]
            intent = self.label_encoder.inverse_transform([predicted_class])[0]
            confidence = tf.nn.softmax(outputs.logits)[0][predicted_class].numpy()
            
            # Should predict weather_alert with reasonable confidence
            assert intent == 'weather_alert', f"Expected 'weather_alert', got '{intent}' for query: {query}"
            assert confidence > 0.2, f"Low confidence ({confidence:.3f}) for query: {query}"
    
    def test_intent_prediction_mitigation_help(self):
        """Test prediction for mitigation_help intent."""
        queries = [
            "What should I do about delays?",
            "How to avoid supply chain risks?",
            "Suggest alternative routes"
        ]
        
        for query in queries:
            inputs = self.tokenizer(query, return_tensors='tf', truncation=True, padding=True, max_length=128)
            outputs = self.model(inputs)
            predicted_class = tf.argmax(outputs.logits, axis=1).numpy()[0]
            intent = self.label_encoder.inverse_transform([predicted_class])[0]
            confidence = tf.nn.softmax(outputs.logits)[0][predicted_class].numpy()
            
            # Should predict mitigation_help with reasonable confidence
            assert intent in ['mitigation_help', 'risk_check'], f"Expected 'mitigation_help' (or similar), got '{intent}' for query: {query}"
            assert confidence > 0.2, f"Low confidence ({confidence:.3f}) for query: {query}"
    
    def test_intent_prediction_general_query(self):
        """Test prediction for general_query intent."""
        queries = [
            "Hello, how can you help?",
            "What can this system do?",
            "Tell me about your capabilities"
        ]
        
        for query in queries:
            inputs = self.tokenizer(query, return_tensors='tf', truncation=True, padding=True, max_length=128)
            outputs = self.model(inputs)
            predicted_class = tf.argmax(outputs.logits, axis=1).numpy()[0]
            intent = self.label_encoder.inverse_transform([predicted_class])[0]
            confidence = tf.nn.softmax(outputs.logits)[0][predicted_class].numpy()
            
            # Should predict general_query with reasonable confidence
            assert intent == 'general_query', f"Expected 'general_query', got '{intent}' for query: {query}"
            assert confidence > 0.2, f"Low confidence ({confidence:.3f}) for query: {query}"
    
    def test_model_output_shape(self):
        """Test model output has correct shape."""
        sample_text = "Test query"
        inputs = self.tokenizer(sample_text, return_tensors='tf', truncation=True, padding=True, max_length=128)
        outputs = self.model(inputs)
        
        # Should have logits for all intent classes
        expected_num_classes = len(self.label_encoder.classes_)
        assert outputs.logits.shape[1] == expected_num_classes
        assert outputs.logits.shape[0] == 1  # Batch size of 1
    
    def test_confidence_scores(self):
        """Test confidence scores are properly normalized."""
        sample_text = "What's the risk for Mumbai?"
        inputs = self.tokenizer(sample_text, return_tensors='tf', truncation=True, padding=True, max_length=128)
        outputs = self.model(inputs)
        probabilities = tf.nn.softmax(outputs.logits)[0].numpy()
        
        # Probabilities should sum to approximately 1
        assert abs(np.sum(probabilities) - 1.0) < 0.01
        # All probabilities should be between 0 and 1
        assert all(0 <= p <= 1 for p in probabilities)
    
    def test_empty_query_handling(self):
        """Test model handles empty or very short queries."""
        queries = ["", "Hi", "?"]
        
        for query in queries:
            inputs = self.tokenizer(query, return_tensors='tf', truncation=True, padding=True, max_length=128)
            outputs = self.model(inputs)
            predicted_class = tf.argmax(outputs.logits, axis=1).numpy()[0]
            intent = self.label_encoder.inverse_transform([predicted_class])[0]
            
            # Should return some valid intent (not crash)
            assert intent in self.label_encoder.classes_
    
    def test_long_query_handling(self):
        """Test model handles very long queries."""
        long_query = "What is the risk assessment for supply chain disruptions " * 20
        inputs = self.tokenizer(long_query, return_tensors='tf', truncation=True, padding=True, max_length=128)
        outputs = self.model(inputs)
        predicted_class = tf.argmax(outputs.logits, axis=1).numpy()[0]
        intent = self.label_encoder.inverse_transform([predicted_class])[0]
        
        # Should return some valid intent (not crash due to truncation)
        assert intent in self.label_encoder.classes_
        # Input should be truncated to max_length
        assert inputs['input_ids'].shape[1] == 128

# Run tests if called directly
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
