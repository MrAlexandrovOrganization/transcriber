"""Inference shared by local and remote workers; transport remains independent."""

from __future__ import annotations

import copy
import dataclasses
import os
import threading
from collections.abc import Callable

from whisper.scheduling import worker_concurrency

_VOICE_PARAMS: dict = {
    "language": "ru",
    "beam_size": 5,
    "temperature": 0.0,
    "vad_filter": True,
    "vad_parameters": {"min_silence_duration_ms": 500, "speech_pad_ms": 200},
    "condition_on_previous_text": True,
    "initial_prompt": "Разговорная речь на русском языке. Текст с правильными знаками препинания.",
    "repetition_penalty": 1.2,
    "no_speech_threshold": 0.5,
}

_LECTURE_PARAMS: dict = {
    "language": "ru",
    "beam_size": 7,
    "temperature": 0.0,
    "vad_filter": True,
    "vad_parameters": {"min_silence_duration_ms": 500, "speech_pad_ms": 200},
    "condition_on_previous_text": True,
    "initial_prompt": (
        "Академическая лекция на русском языке. "
        "Текст с правильными знаками препинания и заглавными буквами."
    ),
    "repetition_penalty": 1.2,
    "hallucination_silence_threshold": 3.0,
    "no_speech_threshold": 0.5,
}

_MEETING_PARAMS: dict = {
    "language": "ru",
    "beam_size": 5,
    "temperature": 0.0,
    "vad_filter": True,
    "vad_parameters": {"min_silence_duration_ms": 300, "speech_pad_ms": 150},
    "condition_on_previous_text": True,
    "initial_prompt": "Деловая встреча или разговор на русском языке. Текст с правильными знаками препинания.",
    "repetition_penalty": 1.2,
    "no_speech_threshold": 0.4,
}

PRESETS: dict[str, dict] = {
    "auto": _VOICE_PARAMS,
    "voice": _VOICE_PARAMS,
    "lecture": _LECTURE_PARAMS,
    "meeting": _MEETING_PARAMS,
}


class InferenceStopped(Exception):
    """The caller stopped the task or lost its lease."""


@dataclasses.dataclass(frozen=True)
class TranscriptionResult:
    text: str
    segments: list[dict[str, float | str]]


class FasterWhisperEngine:
    def __init__(self, *, concurrency: int | None = None):
        from faster_whisper import WhisperModel

        device = os.getenv("WHISPER_DEVICE", "cpu").strip().lower()
        if device not in {"cpu", "cuda"}:
            raise ValueError("WHISPER_DEVICE must be cpu or cuda")
        threads = int(os.getenv("WHISPER_CPU_THREADS", "2"))
        if threads < 1:
            raise ValueError("WHISPER_CPU_THREADS must be positive")
        concurrency = worker_concurrency(
            str(concurrency)
            if concurrency is not None
            else os.getenv("WORKER_CONCURRENCY", "1")
        )
        self._model = WhisperModel(
            os.getenv("WHISPER_MODEL", "small"),
            device=device,
            compute_type=os.getenv(
                "WHISPER_COMPUTE_TYPE", "int8" if device == "cpu" else "float16"
            ),
            cpu_threads=threads,
            # CTranslate2 shares model weights between execution workers on
            # the same device. Every call keeps its own decoder/result state.
            num_workers=concurrency,
            local_files_only=os.getenv("WHISPER_LOCAL_FILES_ONLY", "0") == "1",
        )

    def transcribe(
        self,
        input_path: str,
        *,
        preset: str = "voice",
        language: str = "",
        initial_prompt: str = "",
        on_progress: Callable[[float], None] = lambda _: None,
        should_stop: threading.Event | None = None,
    ) -> TranscriptionResult:
        params = copy.deepcopy(PRESETS.get(preset, PRESETS["voice"]))
        if language:
            params["language"] = language
        if initial_prompt:
            params["initial_prompt"] = initial_prompt
        if should_stop is not None and should_stop.is_set():
            raise InferenceStopped()
        segments_iter, info = self._model.transcribe(input_path, **params)
        total_duration = max(float(getattr(info, "duration", 0.0) or 0.0), 0.0)
        parts: list[str] = []
        segments: list[dict[str, float | str]] = []
        for segment in segments_iter:
            if should_stop is not None and should_stop.is_set():
                raise InferenceStopped()
            text = segment.text.strip()
            parts.append(text)
            if preset == "lecture":
                segments.append(
                    {
                        "start": float(segment.start),
                        "end": float(segment.end),
                        "text": text,
                    }
                )
            if total_duration:
                on_progress(
                    min(max(float(segment.end) / total_duration * 100.0, 0.0), 99.0)
                )
        if should_stop is not None and should_stop.is_set():
            raise InferenceStopped()
        return TranscriptionResult(text=" ".join(parts).strip(), segments=segments)
