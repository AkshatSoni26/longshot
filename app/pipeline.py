"""The actual business work — fetching, chunking, summarizing.

Three LLM modes (chosen via ``LLM_MODE`` env var):

  * ``mock``     — deterministic stub. No network. Default.
  * ``real``     — Anthropic Messages API.
  * ``lmstudio`` — local LM Studio server, OpenAI-compatible endpoint.

Adding a new mode = one ``case`` in ``_summarize_dispatch`` plus a typed
helper. No ``Any``, no untyped responses; LM Studio's chat-completions JSON
is parsed into Pydantic models at the boundary.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import AsyncIterator
from typing import Literal

import httpx
from pydantic import BaseModel, Field
from selectolax.parser import HTMLParser

from app.settings import LLMMode, get_settings

# Matches a single ``<think>...</think>`` block (non-greedy, multi-line). Used
# to strip reasoning traces emitted by thinking models (Qwen3-Thinking,
# DeepSeek-R1, etc.) before returning the summary.
_THINK_BLOCK_RE = re.compile(r"<think>.*?</think>\s*", re.DOTALL | re.IGNORECASE)


class FetchError(Exception):
    """Anything that prevented us from getting clean text out of a URL."""


class SummarizeError(Exception):
    """Any failure of the LLM call itself (HTTP, timeout, malformed response)."""


# ---------------------------------------------------------------------------
# Fetch & chunk
# ---------------------------------------------------------------------------


async def fetch_text(url: str, *, timeout_s: float = 15.0) -> str:
    """Fetch a URL and return cleaned plain text. Raises FetchError on any failure."""
    try:
        async with httpx.AsyncClient(
            timeout=timeout_s,
            follow_redirects=True,
            headers={"User-Agent": "longshot-demo/0.1 (+https://github.com/AkshatSoni26/longshot)"},
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise FetchError(f"HTTP {exc.response.status_code} from {url}") from exc
    except httpx.RequestError as exc:
        raise FetchError(f"Request failed: {exc}") from exc

    content_type = response.headers.get("content-type", "")
    if "html" in content_type:
        return _strip_html(response.text)
    if "text" in content_type or "json" in content_type:
        return response.text
    raise FetchError(f"Unsupported content-type: {content_type}")


def _strip_html(html: str) -> str:
    tree = HTMLParser(html)
    for sel in ("script", "style", "noscript", "header", "footer", "nav", "aside"):
        for node in tree.css(sel):
            node.decompose()
    body = tree.body or tree.root
    if body is None:
        return ""
    text = body.text(separator=" ", strip=True)
    return " ".join(text.split())


def chunk_text(text: str, *, size: int, max_chunks: int) -> list[str]:
    """Split text into roughly equal char-windows. Boring on purpose."""
    if not text:
        return []
    chunks = [text[i : i + size] for i in range(0, len(text), size)]
    return chunks[:max_chunks]


# ---------------------------------------------------------------------------
# Summarize — typed dispatch on LLMMode
# ---------------------------------------------------------------------------


async def summarize_chunk(chunk: str, *, index: int) -> str:
    return await _summarize_dispatch(
        prompt=f"Summarize the following passage in 1-2 sentences:\n\n{chunk}",
        chunk_for_mock=chunk,
        index=index,
    )


async def _summarize_dispatch(*, prompt: str, chunk_for_mock: str, index: int) -> str:
    """Single dispatch point — exhaustive over ``LLMMode``."""
    mode = get_settings().llm_mode
    match mode:
        case LLMMode.MOCK:
            return await _summarize_mock(chunk_for_mock, index=index)
        case LLMMode.REAL:
            return await _summarize_with_anthropic(prompt)
        case LLMMode.LMSTUDIO:
            return await _summarize_with_lmstudio(prompt)


async def _summarize_mock(chunk: str, *, index: int) -> str:
    # Simulated work — just enough to make progress events feel alive.
    await asyncio.sleep(0.8)
    preview = chunk[:80].replace("\n", " ")
    return f"[chunk #{index}] {len(chunk)} chars summarized — opening: {preview}…"


# ---------------------------------------------------------------------------
# Anthropic
# ---------------------------------------------------------------------------


async def _summarize_with_anthropic(prompt: str) -> str:
    # Lazy import: the ``anthropic`` package is an optional extra.
    try:
        from anthropic import AsyncAnthropic
    except ImportError as exc:  # pragma: no cover
        raise SummarizeError(
            "LLM_MODE=real but the 'anthropic' package isn't installed. "
            "Run: uv sync --extra real"
        ) from exc

    settings = get_settings()
    api_key = os.getenv("ANTHROPIC_API_KEY") or settings.anthropic_api_key
    if not api_key:
        raise SummarizeError("LLM_MODE=real requires ANTHROPIC_API_KEY")

    client = AsyncAnthropic(api_key=api_key)
    msg = await client.messages.create(
        model=settings.anthropic_model,
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}],
    )
    block = msg.content[0]
    if block.type != "text":
        raise SummarizeError(f"Unexpected response block type: {block.type}")
    return block.text


# ---------------------------------------------------------------------------
# LM Studio (OpenAI-compatible)
# ---------------------------------------------------------------------------


class _ChatMessage(BaseModel):
    """OpenAI-compatible chat message — request side."""

    role: Literal["system", "user", "assistant"]
    content: str


class _ChatCompletionRequest(BaseModel):
    """OpenAI-compatible chat-completion request. ``stream`` toggles SSE."""

    model: str
    messages: list[_ChatMessage]
    temperature: float = Field(default=0.3, ge=0.0, le=2.0)
    max_tokens: int = Field(default=2048, ge=1)
    stream: bool = False


class _ChatChoiceMessage(BaseModel):
    role: str
    content: str


class _ChatChoice(BaseModel):
    index: int
    message: _ChatChoiceMessage
    finish_reason: str | None = None


class _ChatCompletionResponse(BaseModel):
    """Subset of OpenAI's chat-completion response — what we actually consume."""

    id: str = ""
    model: str = ""
    choices: list[_ChatChoice]


async def _summarize_with_lmstudio(prompt: str) -> str:
    settings = get_settings()
    base_url = settings.lmstudio_base_url.rstrip("/")
    # Empty model = let LM Studio route to whatever's loaded. Some LM Studio
    # versions reject "" — we send a hyphen which is treated as "default" too.
    model = settings.lmstudio_model or "lmstudio-loaded"

    payload = _ChatCompletionRequest(
        model=model,
        messages=[
            _ChatMessage(
                role="system",
                content=(
                    "You are a concise summarization assistant. "
                    "Reply with the summary only — no preamble, no meta-commentary."
                ),
            ),
            _ChatMessage(role="user", content=prompt),
        ],
        temperature=settings.lmstudio_temperature,
        max_tokens=settings.lmstudio_max_tokens,
    )

    try:
        async with httpx.AsyncClient(timeout=settings.lmstudio_timeout_seconds) as client:
            response = await client.post(
                f"{base_url}/chat/completions",
                json=payload.model_dump(),
            )
    except httpx.RequestError as exc:
        raise SummarizeError(
            f"LM Studio unreachable at {base_url}. Is the server running and is a model loaded? "
            f"({exc})"
        ) from exc

    if response.status_code >= 400:
        raise SummarizeError(
            f"LM Studio returned HTTP {response.status_code}: {response.text[:300]}"
        )

    parsed = _ChatCompletionResponse.model_validate_json(response.content)
    if not parsed.choices:
        raise SummarizeError("LM Studio response had zero choices")
    raw_content = parsed.choices[0].message.content
    return _postprocess_lmstudio(raw_content, strip_thinking=settings.lmstudio_strip_thinking)


def _postprocess_lmstudio(content: str, *, strip_thinking: bool) -> str:
    """Clean LM Studio chat-completion content for use as a summary.

    Thinking models (e.g. Qwen3-Thinking, DeepSeek-R1) emit
    ``<think>...</think>`` blocks before the answer. We strip them so the
    summary the client sees is just the conclusion.
    """
    text = content
    if strip_thinking and "<think>" in text.lower():
        text = _THINK_BLOCK_RE.sub("", text)
        # Defend against unbalanced ``<think>`` (no closing tag) when the model
        # was cut off by max_tokens — drop everything up to the first closing
        # tag if any, else everything.
        if "<think>" in text.lower():
            close_at = text.lower().find("</think>")
            text = text[close_at + len("</think>") :] if close_at != -1 else ""
    return text.strip()


# ---------------------------------------------------------------------------
# Streaming chat — token-by-token over an async generator
# ---------------------------------------------------------------------------


class _StreamDelta(BaseModel):
    """A single OpenAI-compatible chat-completion stream chunk's delta."""

    role: str | None = None
    content: str | None = None


class _StreamChoice(BaseModel):
    index: int = 0
    delta: _StreamDelta = Field(default_factory=_StreamDelta)
    finish_reason: str | None = None


class _StreamChunk(BaseModel):
    """One ``data: {...}`` event from an OpenAI-compatible streaming response."""

    choices: list[_StreamChoice]


async def chat_stream(prompt: str, *, system: str | None = None) -> AsyncIterator[str]:
    """Stream an LLM response token-by-token. One ``case`` per LLM mode.

    Yields incremental ``str`` deltas; the caller concatenates them into the
    full answer. Raises ``SummarizeError`` on transport / API failures.
    """
    mode = get_settings().llm_mode
    match mode:
        case LLMMode.MOCK:
            async for d in _stream_mock(prompt):
                yield d
        case LLMMode.LMSTUDIO:
            async for d in _stream_lmstudio(prompt, system=system):
                yield d
        case LLMMode.REAL:
            async for d in _stream_anthropic(prompt, system=system):
                yield d


async def _stream_mock(prompt: str) -> AsyncIterator[str]:
    """Deterministic streaming stub. Builds a canned reply, emits it word-by-word
    so the front-end has something to render."""
    snippet = prompt.replace("\n", " ")[:120]
    reply = (
        "This is a mock streaming response. The mock LLM has no actual "
        "reasoning ability, but it does demonstrate token-by-token rendering "
        f"end to end. You asked about: {snippet!r}."
    )
    for word in reply.split(" "):
        await asyncio.sleep(0.04)
        yield word + " "


async def _stream_lmstudio(prompt: str, *, system: str | None) -> AsyncIterator[str]:
    """Stream from an OpenAI-compatible /v1/chat/completions endpoint with
    ``stream: true``. Strips ``<think>`` blocks from the live deltas so
    thinking-model reasoning never reaches the user."""
    settings = get_settings()
    base_url = settings.lmstudio_base_url.rstrip("/")
    model = settings.lmstudio_model or "lmstudio-loaded"

    messages: list[_ChatMessage] = []
    if system:
        messages.append(_ChatMessage(role="system", content=system))
    messages.append(_ChatMessage(role="user", content=prompt))

    payload = _ChatCompletionRequest(
        model=model,
        messages=messages,
        temperature=settings.lmstudio_temperature,
        max_tokens=settings.lmstudio_max_tokens,
        stream=True,
    )

    async with httpx.AsyncClient(timeout=settings.lmstudio_timeout_seconds) as client:
        try:
            async with client.stream(
                "POST", f"{base_url}/chat/completions", json=payload.model_dump()
            ) as response:
                if response.status_code >= 400:
                    text = await response.aread()
                    raise SummarizeError(
                        f"LM Studio HTTP {response.status_code}: {text.decode()[:300]}"
                    )
                async for delta in _iter_openai_stream(
                    response, strip_thinking=settings.lmstudio_strip_thinking
                ):
                    yield delta
        except httpx.RequestError as exc:
            raise SummarizeError(
                f"LM Studio unreachable at {base_url}. Is the server running and "
                f"is a model loaded? ({exc})"
            ) from exc


async def _iter_openai_stream(
    response: httpx.Response, *, strip_thinking: bool
) -> AsyncIterator[str]:
    """Parse the SSE-shaped stream-of-JSON from an OpenAI-compatible endpoint.

    Live ``<think>`` stripping: while we're still inside a think block we
    swallow tokens instead of yielding them. The user sees the answer only.
    """
    in_think = False
    async for raw in response.aiter_lines():
        line = raw.strip()
        if not line or not line.startswith("data:"):
            continue
        data = line[len("data:") :].strip()
        if data == "[DONE]":
            return
        try:
            chunk = _StreamChunk.model_validate_json(data)
        except Exception:
            continue
        if not chunk.choices:
            continue
        delta = chunk.choices[0].delta.content or ""
        if not delta:
            continue
        if strip_thinking:
            delta, in_think = _strip_thinking_streaming(delta, in_think)
            if not delta:
                continue
        yield delta


def _strip_thinking_streaming(delta: str, in_think: bool) -> tuple[str, bool]:
    """Single-pass scanner over a streamed delta that suppresses anything
    between ``<think>`` and ``</think>`` tags. Returns the surviving text plus
    the new ``in_think`` state to thread through the next call."""
    out: list[str] = []
    i = 0
    while i < len(delta):
        if in_think:
            close = delta.lower().find("</think>", i)
            if close == -1:
                return "".join(out), True
            i = close + len("</think>")
            in_think = False
            continue
        opn = delta.lower().find("<think>", i)
        if opn == -1:
            out.append(delta[i:])
            return "".join(out), False
        out.append(delta[i:opn])
        i = opn + len("<think>")
        in_think = True
    return "".join(out), in_think


async def _stream_anthropic(prompt: str, *, system: str | None) -> AsyncIterator[str]:
    """Stream from Anthropic's Messages API. Lazy-imports so the optional
    extra is truly optional."""
    try:
        from anthropic import AsyncAnthropic
    except ImportError as exc:  # pragma: no cover
        raise SummarizeError(
            "LLM_MODE=real but the 'anthropic' package isn't installed. "
            "Run: uv sync --extra real"
        ) from exc

    settings = get_settings()
    api_key = os.getenv("ANTHROPIC_API_KEY") or settings.anthropic_api_key
    if not api_key:
        raise SummarizeError("LLM_MODE=real requires ANTHROPIC_API_KEY")

    client = AsyncAnthropic(api_key=api_key)
    kwargs: dict[str, str | int | list[dict[str, str]]] = {
        "model": settings.anthropic_model,
        "max_tokens": 1024,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        kwargs["system"] = system
    async with client.messages.stream(**kwargs) as stream:  # type: ignore[arg-type]
        async for text in stream.text_stream:
            yield text


__all__ = [
    "FetchError",
    "SummarizeError",
    "chat_stream",
    "chunk_text",
    "fetch_text",
    "summarize_chunk",
]

