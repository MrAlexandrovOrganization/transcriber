"""Synthetic configuration and shared-model concurrency checks."""

import os
import sys
import threading
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

from whisper.inference import FasterWhisperEngine
from whisper.scheduling import parse_policies, worker_concurrency


class SchedulingConfigTests(unittest.TestCase):
    def test_policies_validate_without_exposing_bad_input(self):
        self.assertEqual(parse_policies(""), {})
        self.assertEqual(
            parse_policies('{"wsl-cpu":{"priority":10,"max_concurrency":2}}')[
                "wsl-cpu"
            ]["priority"],
            10,
        )
        for invalid in (
            "accidental-secret",
            "[]",
            "{}",
            '{"wsl-cpu":{"priority":true,"max_concurrency":2}}',
            '{"wsl-cpu":{"priority":-1,"max_concurrency":2}}',
            '{"wsl-cpu":{"priority":1,"max_concurrency":33}}',
            '{"wsl-cpu":{"priority":1}}',
            '{"other":{"priority":1,"max_concurrency":1}}',
        ):
            with (
                self.subTest(invalid=invalid),
                self.assertRaisesRegex(
                    ValueError, "^Invalid WORKER_POLICIES configuration$"
                ),
            ):
                parse_policies(invalid)

    def test_concurrency_is_bounded(self):
        for value in ("0", "17", "2.5", "secret"):
            with self.assertRaisesRegex(ValueError, "WORKER_CONCURRENCY must"):
                worker_concurrency(value)
        self.assertEqual(worker_concurrency("3"), 3)

    def test_one_model_handles_parallel_calls_with_isolated_options_and_results(self):
        rendezvous = threading.Barrier(2)
        options = {}

        def transcribe(path, **kwargs):
            options[path] = kwargs
            rendezvous.wait(timeout=3)
            segment = types.SimpleNamespace(text=path, start=0, end=1)
            return iter([segment]), types.SimpleNamespace(duration=1)

        model = Mock(transcribe=transcribe)
        constructor = Mock(return_value=model)
        with (
            patch.dict(
                sys.modules,
                {"faster_whisper": types.SimpleNamespace(WhisperModel=constructor)},
            ),
            patch.dict(
                os.environ, {"WHISPER_DEVICE": "cpu", "WHISPER_CPU_THREADS": "2"}
            ),
        ):
            engine = FasterWhisperEngine(concurrency=2)
        constructor.assert_called_once()
        self.assertEqual(constructor.call_args.kwargs["num_workers"], 2)
        self.assertEqual(constructor.call_args.kwargs["cpu_threads"], 2)
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(
                engine.transcribe, "first", language="en", preset="lecture"
            )
            second = executor.submit(
                engine.transcribe, "second", language="ru", preset="voice"
            )
            self.assertEqual(first.result().text, "first")
            self.assertEqual(len(first.result().segments), 1)
            self.assertEqual(second.result().text, "second")
            self.assertEqual(second.result().segments, [])
        self.assertEqual(options["first"]["language"], "en")
        self.assertEqual(options["second"]["language"], "ru")
        self.assertIsNot(
            options["first"]["vad_parameters"], options["second"]["vad_parameters"]
        )
