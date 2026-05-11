from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any
from urllib import error, request

from dotenv import load_dotenv

from categorize import MUU_CATEGORY, classify

load_dotenv()


@dataclass
class OllamaConfig:
    provider: str = "ollama"
    base_url: str = "http://127.0.0.1:11434"
    model: str = "llama3.2:3b"
    timeout_seconds: float = 30.0
    temperature: float = 0.0


@dataclass
class HybridRoutingStats:
    rows_resolved_by_rule: int = 0
    rows_sent_to_llm: int = 0

    def as_dict(self) -> dict[str, Any]:
        total = self.rows_resolved_by_rule + self.rows_sent_to_llm
        return {
            "rows_resolved_by_rule": self.rows_resolved_by_rule,
            "rows_sent_to_llm": self.rows_sent_to_llm,
            "share_resolved_by_rule": (self.rows_resolved_by_rule / total) if total else 0.0,
            "share_sent_to_llm": (self.rows_sent_to_llm / total) if total else 0.0,
        }


@dataclass
class TokenUsageStats:
    total_prompt_tokens: int = 0
    total_completion_tokens: int = 0
    items_with_llm_calls: int = 0

    def add(self, prompt_tokens: int | None, completion_tokens: int | None) -> None:
        p = int(prompt_tokens or 0)
        c = int(completion_tokens or 0)
        self.total_prompt_tokens += p
        self.total_completion_tokens += c
        self.items_with_llm_calls += 1
        print(
            f"[token_usage] prompt_tokens={p} completion_tokens={c}"
        )

    def as_dict(self) -> dict[str, float | int]:
        avg = (
            (self.total_prompt_tokens + self.total_completion_tokens) / self.items_with_llm_calls
            if self.items_with_llm_calls
            else 0.0
        )
        return {
            "total_prompt_tokens": self.total_prompt_tokens,
            "total_completion_tokens": self.total_completion_tokens,
            "avg_tokens_per_item": float(avg),
        }


@dataclass
class EvaluationPredictor:
    approach: str
    labels: list[str]
    ollama: OllamaConfig
    _cache: dict[str, str] = field(default_factory=dict)
    hybrid_stats: HybridRoutingStats = field(default_factory=HybridRoutingStats)
    token_usage: TokenUsageStats = field(default_factory=TokenUsageStats)
    _claude_debug_logged_requests: int = 0
    _gemini_debug_logged_errors: int = 0
    _openai_debug_logged_errors: int = 0
    _openai_next_request_not_before: float = 0.0
    _claude_batch_debug_logged_events: int = 0

    def prefill_batch_cache(self, item_texts: list[str], chunk_size: int = 120) -> None:
        provider = str(self.ollama.provider or "").strip().lower()
        if self.approach != "llm" or provider not in {"openai", "gemini", "deepseek", "claude"}:
            return
        unique_items = self._dedupe_uncached_items(item_texts)
        if not unique_items:
            return
        safe_chunk_size = max(20, int(chunk_size or 120))
        if provider == "claude":
            # Claude responses can hit output token limits on large JSON batches.
            safe_chunk_size = min(safe_chunk_size, 30)
        for idx in range(0, len(unique_items), safe_chunk_size):
            chunk = unique_items[idx: idx + safe_chunk_size]
            predictions = self._call_batch_for_provider(provider, chunk)
            for item in chunk:
                self._cache[item] = predictions.get(item, MUU_CATEGORY)

    def predict(self, item_text: str, is_deposit: bool) -> str:
        if self.approach == "rule":
            cat, _ = classify(item_text, is_deposit)
            return cat

        if is_deposit:
            cat, _ = classify(item_text, True)
            return cat

        if self.approach == "llm":
            return self._predict_llm(item_text)

        # hybrid: first rule, then LLM only on fallback (no matched keyword).
        cat, matched_kw = classify(item_text, False)
        if matched_kw:
            self.hybrid_stats.rows_resolved_by_rule += 1
            return cat
        self.hybrid_stats.rows_sent_to_llm += 1
        return self._predict_llm(item_text)

    def _predict_llm(self, item_text: str) -> str:
        key = str(item_text or "").strip()
        if key in self._cache:
            return self._cache[key]

        predicted = self._call_ollama_and_parse(key)
        self._cache[key] = predicted
        return predicted

    def _dedupe_uncached_items(self, item_texts: list[str]) -> list[str]:
        cleaned_items = [str(item or "").strip() for item in item_texts]
        unique_items: list[str] = []
        seen: set[str] = set()
        for item in cleaned_items:
            if not item or item in seen:
                continue
            seen.add(item)
            if item not in self._cache:
                unique_items.append(item)
        return unique_items

    def _call_batch_for_provider(self, provider: str, item_texts: list[str]) -> dict[str, str]:
        if provider == "openai":
            return self._call_openai_batch(item_texts)
        if provider == "deepseek":
            return self._call_deepseek_batch(item_texts)
        if provider == "gemini":
            return self._call_gemini_batch(item_texts)
        if provider == "claude":
            return self._call_claude_batch(item_texts)
        return {item: MUU_CATEGORY for item in item_texts}

    def _call_ollama_and_parse(self, item_text: str) -> str:
        provider = str(self.ollama.provider or "ollama").strip().lower()
        prompt = self._build_prompt(item_text)

        if provider == "ollama":
            return self._call_ollama(prompt)
        if provider == "gemini":
            return self._call_gemini(prompt)
        if provider == "deepseek":
            return self._call_deepseek(prompt)
        if provider == "openai":
            return self._call_openai(prompt)
        if provider == "claude":
            return self._call_claude(prompt)
        return MUU_CATEGORY

    def _build_prompt(self, item_text: str) -> str:
        model_name = str(self.ollama.model or "").strip().lower()
        if model_name.startswith("mistral"):
            return (
                "You are a classifier for grocery store receipt line items.\n"
                "You MUST return ONLY one of these EXACT category strings, copied verbatim:\n"
                "- Toidukaubad ja alkoholivabad joogid\n"
                "- Alkohol ja tubakas\n"
                "- Majapidamis- ja puhastusvahendid\n"
                "- Majapidamistehnika\n"
                "- Lilled ja kingitused\n"
                "- Muu\n\n"
                "DO NOT translate. DO NOT create new categories. Copy the string exactly.\n\n"
                "Examples:\n"
                'Item: "Piim 3,2% 1L" -> {"category": "Toidukaubad ja alkoholivabad joogid"}\n'
                'Item: "ÕLUT SAKU 0.5L" -> {"category": "Alkohol ja tubakas"}\n'
                'Item: "PESUPULBER 3KG" -> {"category": "Majapidamis- ja puhastusvahendid"}\n\n'
                'Return ONLY JSON: {"category": "<exact string from list above>"}\n'
                f'Item: "{item_text}"'
            )
        return (
            "You are a classifier for grocery store receipt line items.\n"
            "Classify the item into exactly one category.\n\n"
            "Categories:\n"
            "- Toidukaubad ja alkoholivabad joogid: food, beverages (non-alcoholic), snacks, dairy, meat, bread\n"
            "- Alkohol ja tubakas: beer, wine, spirits, tobacco, cigarettes\n"
            "- Majapidamis- ja puhastusvahendid: cleaning products, detergents, household supplies, paper products\n"
            "- Majapidamistehnika: appliances, electronics, kitchen equipment\n"
            "- Lilled ja kingitused: flowers, gifts, greeting cards\n"
            "- Muu: anything that does not fit the above\n\n"
            "Examples:\n"
            'Item: "PIIM 2.5% 1L" -> {"category": "Toidukaubad ja alkoholivabad joogid"}\n'
            'Item: "ÕLUT SAKU 0.5L" -> {"category": "Alkohol ja tubakas"}\n'
            'Item: "PESUPULBER 3KG" -> {"category": "Majapidamis- ja puhastusvahendid"}\n'
            'Item: "ROOS PUNANE" -> {"category": "Lilled ja kingitused"}\n\n'
            'Return ONLY JSON: {"category": "<category>"}\n'
            f'Item: "{item_text}"'
        )

    def _parse_category(self, llm_raw: str | dict[str, Any]) -> str:
        try:
            if isinstance(llm_raw, str):
                text = llm_raw.strip()
                # Some providers (notably Claude) may wrap JSON in markdown fences.
                if text.startswith("```"):
                    text = text[3:].strip()
                    if text.lower().startswith("json"):
                        text = text[4:].strip()
                    if text.endswith("```"):
                        text = text[:-3].strip()
                parsed = json.loads(text)
            elif isinstance(llm_raw, dict):
                parsed = llm_raw
            else:
                return MUU_CATEGORY
            category = str(parsed.get("category", "")).strip()
            if category in self.labels:
                return category
            return MUU_CATEGORY
        except (json.JSONDecodeError, ValueError, TypeError):
            return MUU_CATEGORY

    def _call_ollama(self, prompt: str) -> str:
        payload = {
            "model": self.ollama.model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": self.ollama.temperature,
            },
        }
        try:
            endpoint = f"{self.ollama.base_url.rstrip('/')}/api/generate"
            req = request.Request(
                endpoint,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with request.urlopen(req, timeout=self.ollama.timeout_seconds) as resp:
                body = resp.read().decode("utf-8")
            decoded = json.loads(body)
            prompt_tokens = int(decoded.get("prompt_eval_count", 0) or 0)
            completion_tokens = int(decoded.get("eval_count", 0) or 0)
            self.token_usage.add(prompt_tokens, completion_tokens)
            return self._parse_category(decoded.get("response", ""))
        except (error.URLError, TimeoutError, json.JSONDecodeError, OSError, ValueError):
            self.token_usage.add(0, 0)
            return MUU_CATEGORY

    def _call_gemini(self, prompt: str) -> str:
        try:
            import google.generativeai as genai

            api_key = os.getenv("GEMINI_API_KEY", "")
            if not api_key:
                raise RuntimeError("GEMINI_API_KEY is missing")
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel(self.ollama.model or "gemini-2.5-flash")
            response = model.generate_content(
                prompt,
                generation_config={
                    "temperature": self.ollama.temperature,
                    "response_mime_type": "application/json",
                },
            )
            usage = getattr(response, "usage_metadata", None)
            prompt_tokens = int(getattr(usage, "prompt_token_count", 0) or 0)
            completion_tokens = int(getattr(usage, "candidates_token_count", 0) or 0)
            self.token_usage.add(prompt_tokens, completion_tokens)
            llm_raw = getattr(response, "text", "") or ""
            return self._parse_category(llm_raw)
        except Exception as exc:
            self._gemini_debug_log_first_errors(exc)
            self.token_usage.add(0, 0)
            return MUU_CATEGORY

    def _call_openai_compatible(self, prompt: str, *, model: str, api_key: str, base_url: str | None = None) -> str:
        try:
            from openai import OpenAI

            if not api_key:
                raise RuntimeError("API key is missing")
            client = OpenAI(api_key=api_key, base_url=base_url)
            # OpenAI free/low tiers may have strict RPM (e.g. 3 RPM ~= 20s/request).
            self._respect_openai_pacing()
            response = None
            last_exc: Exception | None = None
            for _attempt in range(4):
                try:
                    response = client.chat.completions.create(
                        model=model,
                        temperature=self.ollama.temperature,
                        response_format={"type": "json_object"},
                        messages=[
                            {"role": "user", "content": prompt},
                        ],
                    )
                    # Keep a small safety buffer between successful calls.
                    self._openai_next_request_not_before = time.time() + 0.5
                    break
                except Exception as exc:
                    last_exc = exc
                    wait_seconds = self._extract_retry_after_seconds(exc)
                    if wait_seconds is None:
                        raise
                    # Respect server hint and apply a small buffer.
                    self._openai_next_request_not_before = max(
                        self._openai_next_request_not_before,
                        time.time() + wait_seconds + 1.0,
                    )
                    self._respect_openai_pacing()

            if response is None:
                assert last_exc is not None
                raise last_exc

            usage = getattr(response, "usage", None)
            prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
            completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
            self.token_usage.add(prompt_tokens, completion_tokens)
            llm_raw = ""
            if getattr(response, "choices", None):
                llm_raw = str(response.choices[0].message.content or "")
            return self._parse_category(llm_raw)
        except Exception as exc:
            self._openai_debug_log_first_errors(exc)
            self.token_usage.add(0, 0)
            return MUU_CATEGORY

    def _respect_openai_pacing(self) -> None:
        now = time.time()
        if now < self._openai_next_request_not_before:
            time.sleep(self._openai_next_request_not_before - now)

    def _extract_retry_after_seconds(self, exc: Exception) -> int | None:
        msg = str(exc)
        # Example: "Please try again in 20s."
        match = re.search(r"try again in\s+(\d+)s", msg, flags=re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except (TypeError, ValueError):
                return None
        return None

    def _call_deepseek(self, prompt: str) -> str:
        return self._call_openai_compatible(
            prompt,
            model=self.ollama.model or "deepseek-chat",
            api_key=os.getenv("DEEPSEEK_API_KEY", ""),
            base_url="https://api.deepseek.com",
        )

    def _call_openai(self, prompt: str) -> str:
        return self._call_openai_compatible(
            prompt,
            model=self.ollama.model or "gpt-5.4-mini",
            api_key=os.getenv("OPENAI_API_KEY", ""),
            base_url=None,
        )

    def _build_batch_prompt(self, item_texts: list[str]) -> str:
        header = (
            "You are a classifier for grocery store receipt line items.\n"
            "Classify EACH item into exactly one category.\n\n"
            "Allowed categories (use exact strings):\n"
            "- Toidukaubad ja alkoholivabad joogid\n"
            "- Alkohol ja tubakas\n"
            "- Majapidamis- ja puhastusvahendid\n"
            "- Majapidamistehnika\n"
            "- Lilled ja kingitused\n"
            "- Muu\n\n"
            "Return ONLY JSON with this exact schema:\n"
            '{"predictions":[{"item_text":"<original item text>","category":"<exact category>"}]}\n\n'
            "Rules:\n"
            "- Keep item_text exactly as provided.\n"
            "- One prediction for each input item.\n"
            "- Do not skip items.\n\n"
            "Items:\n"
        )
        lines = [f"{idx + 1}. {item}" for idx, item in enumerate(item_texts)]
        return header + "\n".join(lines)

    def _response_to_predictions(self, llm_raw: str, expected_items: list[str]) -> dict[str, str]:
        out = {item: MUU_CATEGORY for item in expected_items}
        predictions = self._extract_predictions_list(llm_raw)
        if not predictions:
            return out
        for row in predictions:
            if not isinstance(row, dict):
                continue
            item = str(row.get("item_text", "")).strip()
            category = self._normalize_category(row.get("category", ""))
            if item in out:
                out[item] = category
        return out

    def _extract_predictions_list(self, llm_raw: str) -> list[dict[str, Any]]:
        parsed = self._parse_json_object(llm_raw)
        predictions = parsed.get("predictions", [])
        if isinstance(predictions, list):
            return [row for row in predictions if isinstance(row, dict)]
        return []

    def _call_openai_batch(self, item_texts: list[str]) -> dict[str, str]:
        out = {item: MUU_CATEGORY for item in item_texts}
        try:
            from openai import OpenAI

            api_key = os.getenv("OPENAI_API_KEY", "")
            if not api_key:
                raise RuntimeError("OPENAI_API_KEY is missing")
            client = OpenAI(api_key=api_key)
            prompt = self._build_batch_prompt(item_texts)
            self._respect_openai_pacing()
            response = client.chat.completions.create(
                model=self.ollama.model or "gpt-5.4-mini",
                temperature=self.ollama.temperature,
                response_format={"type": "json_object"},
                messages=[{"role": "user", "content": prompt}],
            )
            self._openai_next_request_not_before = time.time() + 0.5

            usage = getattr(response, "usage", None)
            prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
            completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
            self.token_usage.add(prompt_tokens, completion_tokens)

            llm_raw = ""
            if getattr(response, "choices", None):
                llm_raw = str(response.choices[0].message.content or "")
            return self._response_to_predictions(llm_raw, item_texts)
        except Exception as exc:
            self._openai_debug_log_first_errors(exc)
            self.token_usage.add(0, 0)
            return out

    def _call_deepseek_batch(self, item_texts: list[str]) -> dict[str, str]:
        out = {item: MUU_CATEGORY for item in item_texts}
        try:
            from openai import OpenAI

            api_key = os.getenv("DEEPSEEK_API_KEY", "")
            if not api_key:
                raise RuntimeError("DEEPSEEK_API_KEY is missing")
            client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
            prompt = self._build_batch_prompt(item_texts)
            response = client.chat.completions.create(
                model=self.ollama.model or "deepseek-chat",
                temperature=self.ollama.temperature,
                response_format={"type": "json_object"},
                messages=[{"role": "user", "content": prompt}],
            )
            usage = getattr(response, "usage", None)
            prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
            completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
            self.token_usage.add(prompt_tokens, completion_tokens)
            llm_raw = ""
            if getattr(response, "choices", None):
                llm_raw = str(response.choices[0].message.content or "")
            return self._response_to_predictions(llm_raw, item_texts)
        except Exception as exc:
            self._openai_debug_log_first_errors(exc)
            self.token_usage.add(0, 0)
            return out

    def _call_gemini_batch(self, item_texts: list[str]) -> dict[str, str]:
        out = {item: MUU_CATEGORY for item in item_texts}
        try:
            import google.generativeai as genai

            api_key = os.getenv("GEMINI_API_KEY", "")
            if not api_key:
                raise RuntimeError("GEMINI_API_KEY is missing")
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel(self.ollama.model or "gemini-2.5-flash")
            prompt = self._build_batch_prompt(item_texts)
            response = model.generate_content(
                prompt,
                generation_config={
                    "temperature": self.ollama.temperature,
                    "response_mime_type": "application/json",
                },
            )
            usage = getattr(response, "usage_metadata", None)
            prompt_tokens = int(getattr(usage, "prompt_token_count", 0) or 0)
            completion_tokens = int(getattr(usage, "candidates_token_count", 0) or 0)
            self.token_usage.add(prompt_tokens, completion_tokens)
            llm_raw = getattr(response, "text", "") or ""
            return self._response_to_predictions(llm_raw, item_texts)
        except Exception as exc:
            self._gemini_debug_log_first_errors(exc)
            self.token_usage.add(0, 0)
            return out

    def _call_claude_batch(self, item_texts: list[str]) -> dict[str, str]:
        out = {item: MUU_CATEGORY for item in item_texts}
        try:
            import anthropic

            api_key = os.getenv("ANTHROPIC_API_KEY", "")
            if not api_key:
                raise RuntimeError("ANTHROPIC_API_KEY is missing")
            client = anthropic.Anthropic(api_key=api_key)
            prompt = self._build_batch_prompt(item_texts)
            response = client.messages.create(
                model=self.ollama.model or "claude-haiku-4-5-20251001",
                temperature=self.ollama.temperature,
                max_tokens=7000,
                messages=[{"role": "user", "content": prompt}],
            )
            usage = getattr(response, "usage", None)
            prompt_tokens = int(getattr(usage, "input_tokens", 0) or 0)
            completion_tokens = int(getattr(usage, "output_tokens", 0) or 0)
            self.token_usage.add(prompt_tokens, completion_tokens)
            content = getattr(response, "content", []) or []
            llm_raw = ""
            if content:
                llm_raw = str(getattr(content[0], "text", "") or "")
            predictions_list = self._extract_predictions_list(llm_raw)
            stop_reason = str(getattr(response, "stop_reason", "") or "").strip().lower()
            looks_truncated = (
                stop_reason == "max_tokens"
                or len(predictions_list) < len(item_texts)
            )
            if looks_truncated and len(item_texts) > 1:
                self._claude_batch_debug_log(
                    f"split chunk size={len(item_texts)} stop_reason={stop_reason or 'n/a'} "
                    f"predictions={len(predictions_list)}"
                )
                mid = len(item_texts) // 2
                left = self._call_claude_batch(item_texts[:mid])
                right = self._call_claude_batch(item_texts[mid:])
                return {**left, **right}
            if looks_truncated and len(item_texts) == 1:
                single_item = item_texts[0]
                self._claude_batch_debug_log(
                    "single-item fallback to non-batch call due to truncated/invalid batch response"
                )
                return {single_item: self._call_claude(self._build_prompt(single_item))}
            return self._response_to_predictions(llm_raw, item_texts)
        except Exception:
            self.token_usage.add(0, 0)
            return out

    def _strip_markdown_fences(self, text: str) -> str:
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned[3:].strip()
            if cleaned.lower().startswith("json"):
                cleaned = cleaned[4:].strip()
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3].strip()
        return cleaned

    def _parse_json_object(self, llm_raw: str) -> dict[str, Any]:
        try:
            text = self._strip_markdown_fences(str(llm_raw or ""))
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
            return {}
        except (json.JSONDecodeError, ValueError, TypeError):
            return {}

    def _normalize_category(self, value: Any) -> str:
        category = str(value or "").strip()
        if category in self.labels:
            return category
        return MUU_CATEGORY

    def _call_claude(self, prompt: str) -> str:
        try:
            import anthropic

            api_key = os.getenv("ANTHROPIC_API_KEY", "")
            if not api_key:
                raise RuntimeError("ANTHROPIC_API_KEY is missing")
            client = anthropic.Anthropic(api_key=api_key)
            response = client.messages.create(
                model=self.ollama.model or "claude-haiku-4-5-20251001",
                temperature=self.ollama.temperature,
                max_tokens=128,
                messages=[
                    {"role": "user", "content": prompt},
                ],
            )
            usage = getattr(response, "usage", None)
            prompt_tokens = int(getattr(usage, "input_tokens", 0) or 0)
            completion_tokens = int(getattr(usage, "output_tokens", 0) or 0)
            self.token_usage.add(prompt_tokens, completion_tokens)
            content = getattr(response, "content", []) or []
            llm_raw = ""
            if content:
                llm_raw = str(getattr(content[0], "text", "") or "")
            category = self._parse_category(llm_raw)
            self._claude_debug_log_first_requests(
                prompt=prompt,
                llm_raw=llm_raw,
                category=category,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
            )
            return category
        except Exception:
            self.token_usage.add(0, 0)
            return MUU_CATEGORY

    def _claude_debug_log_first_requests(
        self,
        *,
        prompt: str,
        llm_raw: str,
        category: str,
        prompt_tokens: int,
        completion_tokens: int,
    ) -> None:
        if self._claude_debug_logged_requests >= 5:
            return
        self._claude_debug_logged_requests += 1
        prompt_tail = prompt.splitlines()[-1] if prompt else ""
        raw_preview = llm_raw if len(llm_raw) <= 300 else f"{llm_raw[:300]}..."
        print(
            "[claude_debug] "
            f"idx={self._claude_debug_logged_requests} "
            f"item_line={prompt_tail} "
            f"raw={raw_preview} "
            f"parsed_category={category} "
            f"prompt_tokens={prompt_tokens} "
            f"completion_tokens={completion_tokens}"
        )

    def _gemini_debug_log_first_errors(self, exc: Exception) -> None:
        if self._gemini_debug_logged_errors >= 5:
            return
        self._gemini_debug_logged_errors += 1
        print(
            "[gemini_debug] "
            f"idx={self._gemini_debug_logged_errors} "
            f"error_type={type(exc).__name__} "
            f"error={exc}"
        )

    def _openai_debug_log_first_errors(self, exc: Exception) -> None:
        if self._openai_debug_logged_errors >= 5:
            return
        self._openai_debug_logged_errors += 1
        print(
            "[openai_debug] "
            f"idx={self._openai_debug_logged_errors} "
            f"error_type={type(exc).__name__} "
            f"error={exc}"
        )

    def _claude_batch_debug_log(self, message: str) -> None:
        if self._claude_batch_debug_logged_events >= 8:
            return
        self._claude_batch_debug_logged_events += 1
        print(f"[claude_batch_debug] idx={self._claude_batch_debug_logged_events} {message}")
