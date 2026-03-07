import yaml
import os
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

SEMANTIC_LAYER_PATH = os.path.join(os.path.dirname(__file__), 'yaml')

class SemanticLayerLoader:
    def __init__(self):
        self.entities = {}
        self.metrics = {}
        self.dimensions = {}
        self.business_rules = {}
        self._load_yaml_files()

    def _load_yaml_files(self):
        try:
            # Load Entities
            with open(os.path.join(SEMANTIC_LAYER_PATH, 'entities.yaml'), 'r') as f:
                data = yaml.safe_load(f)
                for entity in data.get('entities', []):
                    self.entities[entity['name']] = entity

            # Load Metrics
            with open(os.path.join(SEMANTIC_LAYER_PATH, 'metrics.yaml'), 'r') as f:
                data = yaml.safe_load(f)
                for metric in data.get('metrics', []):
                    self.metrics[metric['name']] = metric

            # Load Dimensions
            with open(os.path.join(SEMANTIC_LAYER_PATH, 'dimensions.yaml'), 'r') as f:
                data = yaml.safe_load(f)
                for dimension in data.get('dimensions', []):
                    self.dimensions[dimension['name']] = dimension
            
            # Load Business Rules
            with open(os.path.join(SEMANTIC_LAYER_PATH, 'business_rules.yaml'), 'r') as f:
                data = yaml.safe_load(f)
                for rule in data.get('business_rules', []):
                    self.business_rules[rule['name']] = rule
            
            logger.info("Semantic Layer loaded successfully.")
            
        except Exception as e:
            logger.error(f"Error loading Semantic Layer: {e}")
            raise e

    def get_entity(self, name: str) -> Dict[str, Any]:
        return self.entities.get(name)

    def get_metric(self, name: str) -> Dict[str, Any]:
        return self.metrics.get(name)

    def get_dimension(self, name: str) -> Dict[str, Any]:
        return self.dimensions.get(name)
    
    def get_business_rule(self, name: str) -> Dict[str, Any]:
        return self.business_rules.get(name)

loader = SemanticLayerLoader()
