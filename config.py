"""
UdyamGraph — Centralized Configuration
All settings loaded from environment variables with sensible defaults.
"""

import os
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List


class KafkaSettings(BaseSettings):
    """Kafka broker and consumer configuration."""
    bootstrap_servers: str = Field(
        default="kafka:9092",
        description="Comma-separated Kafka broker addresses"
    )
    group_id: str = Field(
        default="udyamgraph-entity-resolution",
        description="Consumer group ID for UdyamGraph"
    )
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 500
    session_timeout_ms: int = 30000

    # Topic names for each department system
    topic_commercial_taxes: str = "dept.commercial-taxes.businesses"
    topic_factories_board: str = "dept.factories-board.licenses"
    topic_shops_establishments: str = "dept.shops-establishments.registrations"

    class Config:
        env_prefix = "KAFKA_"


class Neo4jSettings(BaseSettings):
    """Neo4j graph database configuration."""
    uri: str = Field(default="bolt://neo4j:7687")
    username: str = Field(default="neo4j")
    password: str = Field(default="udyamgraph_dev")
    database: str = Field(default="neo4j")
    max_connection_pool_size: int = 50

    class Config:
        env_prefix = "NEO4J_"


class EntityResolutionSettings(BaseSettings):
    """Thresholds and model configuration for entity resolution."""
    # Confidence calibration thresholds
    auto_link_threshold: float = Field(
        default=0.92,
        description="p >= this → automatic linkage"
    )
    review_threshold: float = Field(
        default=0.65,
        description="p >= this AND < auto_link → human review queue"
    )
    # Below review_threshold → automatic rejection

    # Cost-sensitive learning
    false_positive_cost: float = Field(
        default=5.0,
        description="Penalty weight for false merges (5:1 vs false negatives)"
    )
    false_negative_cost: float = Field(
        default=1.0,
        description="Penalty weight for missed true matches"
    )

    # Model paths
    xgboost_model_path: str = "models/xgb_entity_resolution.json"
    sbert_model_name: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

    # Blocking parameters
    lsh_num_perm: int = 128
    lsh_threshold: float = 0.3
    max_block_size: int = 1000

    class Config:
        env_prefix = "ER_"


class PIISettings(BaseSettings):
    """PII tokenization configuration."""
    hmac_secret_key: str = Field(
        description="HMAC secret for deterministic tokenization. MUST be set via PII_HMAC_SECRET_KEY env var."
    )
    # Karnataka state code for GSTIN validation
    karnataka_state_code: str = "29"

    class Config:
        env_prefix = "PII_"
        
    def __init__(self, **kwargs):
        # Provide fallback only for development
        if "hmac_secret_key" not in kwargs and "PII_HMAC_SECRET_KEY" not in os.environ:
            import warnings
            warnings.warn(
                "PII_HMAC_SECRET_KEY not set! Using insecure default for development only. "
                "NEVER use this in production!",
                UserWarning
            )
            kwargs["hmac_secret_key"] = "INSECURE_DEV_KEY_udyamgraph_2024"
        super().__init__(**kwargs)


class ActivityClassificationSettings(BaseSettings):
    """Activity classification thresholds."""
    dormant_months_threshold: int = Field(
        default=18,
        description="Months of inactivity before business is flagged DORMANT"
    )
    closed_confidence_threshold: float = Field(
        default=0.85,
        description="Minimum confidence to classify as CLOSED"
    )
    model_path: str = "models/activity_classifier.json"

    class Config:
        env_prefix = "ACTIVITY_"


class UdyamGraphConfig:
    """Master configuration aggregating all subsystem settings."""

    def __init__(self):
        self.kafka = KafkaSettings()
        self.neo4j = Neo4jSettings()
        self.entity_resolution = EntityResolutionSettings()
        self.pii = PIISettings()
        self.activity = ActivityClassificationSettings()


# Singleton config instance
config = UdyamGraphConfig()
