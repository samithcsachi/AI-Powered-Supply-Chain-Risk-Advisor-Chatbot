import pytest
import numpy as np
from transformers import DistilBertTokenizerFast, TFDistilBertForTokenClassification
import joblib
from pathlib import Path
import tensorflow as tf

class TestNERModel:
    @classmethod
    def setup_class(cls):
        cls.model_dir = Path(__file__).resolve().parents[1] / "artifacts" / "models" / "nlp_ner"
        # Model, tokenizer, label2id
        cls.model = TFDistilBertForTokenClassification.from_pretrained(cls.model_dir / "ner_model")
        cls.tokenizer = DistilBertTokenizerFast.from_pretrained(cls.model_dir / "ner_tokenizer")
        cls.label2id = joblib.load(cls.model_dir / "label2id.joblib")
        cls.id2label = {i: l for l, i in cls.label2id.items()}
        cls.max_len = 8  # Should match your training window

    def test_model_loading(self):
        assert self.model is not None
        assert self.tokenizer is not None
        assert self.label2id is not None

    def test_entity_prediction(self):
        sentence = "Flood in Mumbai caused delays"
        tokens = sentence.split()
        encoding = self.tokenizer([tokens], is_split_into_words=True, return_tensors='tf', padding='max_length', truncation=True, max_length=self.max_len)
        outputs = self.model({k: v for k, v in encoding.items() if k != "labels"})
        logits = outputs.logits.numpy()[0]
        pred_ids = np.argmax(logits, axis=-1)
        entity_labels = [self.id2label[id] for id in pred_ids[:len(tokens)]]
        # Check for expected entity
        assert "B-LOC" in entity_labels or "B-EVENT" in entity_labels

    def test_padding_mask(self):
        # Padding should be classified (likely) as "O"
        sentence = "Rain in Beijing"
        tokens = sentence.split()
        encoding = self.tokenizer([tokens], is_split_into_words=True, return_tensors='tf', padding='max_length', truncation=True, max_length=self.max_len)
        outputs = self.model({k: v for k, v in encoding.items() if k != "labels"})
        logits = outputs.logits.numpy()[0]
        pred_ids = np.argmax(logits, axis=-1)
        for idx in range(len(tokens), self.max_len):
            assert self.id2label[pred_ids[idx]] == "O"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
