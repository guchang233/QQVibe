"""Compatibility imports for the former monolithic backend.

New code imports backend_service, result_store, qq_source, node_analysis, or
backend_contracts directly. This facade contains no runtime implementation.
"""

from backend_contracts import (
    API_INSIGHT_REVISION, API_JOB_CACHE_LIMIT, API_MODEL_RETRYABLE, API_MODEL_RETRY_MAX,
    API_MODEL_RETRY_SECONDS, API_PORTRAIT_BATCH_ITEMS, API_PORTRAIT_COUNT_KEYS, API_PORTRAIT_INVENTORY_CACHE_BYTES,
    API_PORTRAIT_MAX_BATCHES, API_PORTRAIT_MAX_WIRE_CHARS, API_PORTRAIT_PIECE_CHARS, API_PORTRAIT_REVISION,
    AXES, AccountChangedError, AccountUnavailableError, FINE_LABEL_SCHEMA,
    LOCAL_SOURCE_ID, ModelSourceUnavailable,
    FORECAST_CACHE_LIMIT, FORECAST_SOURCE_WINDOW, ForecastRequestError, GROUNDED_INTENT_EVIDENCE,
    MAX_IMAGE_BYTES, MAX_ISSUED_IMAGES, MBTI_SOURCES, MIN_AXIS_EVIDENCE,
    MIN_AXIS_MARGIN, MIN_PERSONALITY_MESSAGES, MODEL_CONNECTOR_ERRORS, MOODS,
    MessageWindow, MessageWindowBatch, MessagesUnavailableError, PROFILE_METADATA_CACHE_LIMIT,
    QUOTED_REPLY_TYPE, ROOT, SESSION_PREVIEWS, SYSTEM_NAMES,
    affinity, affinity_from_progress, api_insight_scope, api_portrait_add_counts,
    api_portrait_plan, api_portrait_resume_anchor, api_portrait_scope, api_portrait_tail_hashes,
    api_portrait_wire_chars, empty_api_portrait, infer_mbti, mbti_from_totals,
    model_source_failure, mood_from_progress, scope_rank, valid_api_portrait,
    validate_personality_evidence,
)
from backend_service import (
    Backend,
)
from qq_source import (
    QQSource, avatar_candidates, message_id, positive_timestamp,
)
from node_analysis import (
    NodeAnalysis,
)
from result_store import (
    ResultStore,
)
from profile_state import empty_state as empty_profile_state
from profile_signals import historical_mood, keywords_from_texts, style_traits, summary_from_signals, validate_style_evidence

__all__ = [
    'API_INSIGHT_REVISION', 'API_JOB_CACHE_LIMIT', 'API_MODEL_RETRYABLE', 'API_MODEL_RETRY_MAX',
    'API_MODEL_RETRY_SECONDS', 'API_PORTRAIT_BATCH_ITEMS', 'API_PORTRAIT_COUNT_KEYS', 'API_PORTRAIT_INVENTORY_CACHE_BYTES',
    'API_PORTRAIT_MAX_BATCHES', 'API_PORTRAIT_MAX_WIRE_CHARS', 'API_PORTRAIT_PIECE_CHARS', 'API_PORTRAIT_REVISION',
    'AXES', 'AccountChangedError', 'AccountUnavailableError', 'FINE_LABEL_SCHEMA',
    'LOCAL_SOURCE_ID', 'ModelSourceUnavailable',
    'FORECAST_CACHE_LIMIT', 'FORECAST_SOURCE_WINDOW', 'ForecastRequestError', 'GROUNDED_INTENT_EVIDENCE',
    'MAX_IMAGE_BYTES', 'MAX_ISSUED_IMAGES', 'MBTI_SOURCES', 'MIN_AXIS_EVIDENCE',
    'MIN_AXIS_MARGIN', 'MIN_PERSONALITY_MESSAGES', 'MODEL_CONNECTOR_ERRORS', 'MOODS',
    'MessageWindow', 'MessageWindowBatch', 'MessagesUnavailableError', 'PROFILE_METADATA_CACHE_LIMIT',
    'QUOTED_REPLY_TYPE', 'ROOT', 'SESSION_PREVIEWS', 'SYSTEM_NAMES',
    'affinity', 'affinity_from_progress', 'api_insight_scope', 'api_portrait_add_counts',
    'api_portrait_plan', 'api_portrait_resume_anchor', 'api_portrait_scope', 'api_portrait_tail_hashes',
    'api_portrait_wire_chars', 'empty_api_portrait', 'infer_mbti', 'mbti_from_totals',
    'model_source_failure', 'mood_from_progress', 'scope_rank', 'valid_api_portrait',
    'validate_personality_evidence', 'Backend', 'positive_timestamp',
    'avatar_candidates', 'message_id',
    'QQSource', 'NodeAnalysis', 'ResultStore', 'empty_profile_state',
    'historical_mood', 'keywords_from_texts', 'style_traits', 'summary_from_signals',
    'validate_style_evidence',
]
