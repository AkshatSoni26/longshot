from enum import StrEnum
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LLMMode(StrEnum):
    """Where summarization actually happens.

    * MOCK     — deterministic stub, no network. Default; runs anywhere.
    * REAL     — Anthropic Messages API (requires ``ANTHROPIC_API_KEY``).
    * LMSTUDIO — local LM Studio server, OpenAI-compatible endpoint at
                 ``LMSTUDIO_BASE_URL``. Free for local dev/testing.
    """

    MOCK = "mock"
    REAL = "real"
    LMSTUDIO = "lmstudio"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- transport ---------------------------------------------------------
    redis_url: str = "redis://localhost:6379/0"

    # --- LLM ---------------------------------------------------------------
    llm_mode: LLMMode = LLMMode.MOCK

    # Anthropic (LLMMode.REAL)
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-haiku-4-5-20251001"

    # LM Studio (LLMMode.LMSTUDIO)
    # Default targets host LM Studio from inside a Docker container.
    # When running natively, set LMSTUDIO_BASE_URL=http://localhost:1234/v1
    lmstudio_base_url: str = "http://host.docker.internal:1234/v1"
    # Empty string = "use whatever model is loaded". LM Studio routes any
    # request to the loaded model when the name doesn't match.
    lmstudio_model: str = ""
    lmstudio_timeout_seconds: float = 120.0
    # Token cap for each chat-completion call. Thinking models (Qwen3-Thinking,
    # DeepSeek-R1, etc.) burn many tokens before producing the answer — keep
    # this generous. Non-thinking models simply won't use it all.
    lmstudio_max_tokens: int = Field(default=2048, ge=64)
    lmstudio_temperature: float = Field(default=0.3, ge=0.0, le=2.0)
    # Strip ``<think>...</think>`` blocks from responses. Default True so the
    # demo works seamlessly whether the loaded model is a thinking variant
    # (Qwen3-Thinking, DeepSeek-R1) or a regular chat model. Set False if you
    # *want* the reasoning trace surfaced in the summary.
    lmstudio_strip_thinking: bool = True

    # --- broker / worker ---------------------------------------------------
    worker_consumer_name: str = "worker-1"
    consumer_group: str = "longshot-workers"
    stream_name: str = "longshot:tasks"

    # --- timeouts / TTLs ---------------------------------------------------
    task_hard_timeout_seconds: int = Field(default=120, ge=1)
    idempotency_lock_ttl_seconds: int = Field(default=600, ge=1)
    result_ttl_seconds: int = Field(default=3600, ge=1)
    progress_snapshot_ttl_seconds: int = Field(default=3600, ge=1)
    sse_heartbeat_seconds: int = Field(default=30, ge=1)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
