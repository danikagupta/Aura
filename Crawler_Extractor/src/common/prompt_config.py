"""Seed-number scoring prompt configuration helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from .errors import ConfigError

DEFAULT_SEED_PROMPT_TABLE: Dict[int, str] = {
    1: "pharmacogenetics_score_prompt.txt",
    2: "pharmacogenetics_score_prompt.txt",
    3: "heterogeneous_catalyst_prompt.txt",
    4: "thermocatalytic_co2_to_methanol_prompt.txt",
}


class SeedPromptConfig:
    """Maps seed numbers to prompt files stored under `prompts/`."""

    def __init__(
        self,
        *,
        table: Optional[Dict[int, str]] = None,
        prompt_directory: Optional[Path] = None,
    ) -> None:
        self._table = dict(table or DEFAULT_SEED_PROMPT_TABLE)
        base_dir = prompt_directory
        if base_dir is None:
            base_dir = Path(__file__).resolve().parents[2] / "prompts"
        self._prompt_dir = base_dir

    def prompt_text_for_seed(self, seed_number: int) -> str:
        """Return the prompt contents for a configured seed number."""

        path = self._prompt_path_for(seed_number)
        return path.read_text(encoding="utf-8")

    def _prompt_path_for(self, seed_number: int) -> Path:
        file_name = self._table.get(seed_number)
        if not file_name:
            raise ConfigError(f"No scoring prompt configured for seed {seed_number}")
        path = self._prompt_dir / file_name
        if not path.exists():
            raise ConfigError(f"Prompt file not found for seed {seed_number}: {path}")
        return path
