"""Think tag filter for streaming LLM responses.

Filters out <think>...</think> tags from streaming content,
handling cross-chunk boundaries with a state machine approach.
"""

from __future__ import annotations

import re
from enum import Enum, auto


class _State(Enum):
    """State machine states for think tag filtering."""

    NORMAL = auto()  # Outside any think tag
    IN_THINK = auto()  # Inside <think>...</think>
    MAYBE_TAG = auto()  # Potentially starting a tag (saw '<')


class ThinkTagFilter:
    """Streaming filter for <think>...</think> tags.

    Handles:
    - Cross-chunk tag boundaries
    - Case-insensitive matching
    - Nested/malformed tags (greedy matching)
    """

    # Patterns for tag detection (case-insensitive)
    _OPEN_TAG = re.compile(r"<think>", re.IGNORECASE)
    _CLOSE_TAG = re.compile(r"</think>", re.IGNORECASE)

    def __init__(self) -> None:
        self._state = _State.NORMAL
        self._buffer = ""  # Buffer for potential partial tags
        self._depth = 0  # Track nested tags

    def feed(self, chunk: str) -> str:
        """Process a chunk and return filtered content.

        Args:
            chunk: New content chunk from stream

        Returns:
            Filtered content (think tags removed)
        """
        if not chunk:
            return ""

        # Combine buffer with new chunk
        text = self._buffer + chunk
        self._buffer = ""

        output = []
        i = 0

        while i < len(text):
            if self._state == _State.NORMAL:
                # Look for opening tag
                match = self._OPEN_TAG.search(text, i)
                if match:
                    # Output everything before the tag
                    output.append(text[i : match.start()])
                    self._state = _State.IN_THINK
                    self._depth = 1
                    i = match.end()
                else:
                    # Check if we might have a partial tag at the end
                    # Look for '<' that could start '<think>'
                    last_lt = text.rfind("<", max(0, len(text) - 7))
                    if last_lt >= i and last_lt > len(text) - 7:
                        # Potential partial tag, buffer it
                        output.append(text[i:last_lt])
                        self._buffer = text[last_lt:]
                        i = len(text)
                    else:
                        output.append(text[i:])
                        i = len(text)

            elif self._state == _State.IN_THINK:
                # Look for closing tag
                close_match = self._CLOSE_TAG.search(text, i)
                open_match = self._OPEN_TAG.search(text, i)

                if close_match:
                    # Check if there's a nested open before this close
                    if open_match and open_match.start() < close_match.start():
                        self._depth += 1
                        i = open_match.end()
                    else:
                        self._depth -= 1
                        if self._depth <= 0:
                            self._state = _State.NORMAL
                            self._depth = 0
                        i = close_match.end()
                elif open_match:
                    # Nested open tag
                    self._depth += 1
                    i = open_match.end()
                else:
                    # No closing tag found, check for partial
                    last_lt = text.rfind("<", max(0, len(text) - 8))
                    if last_lt >= i and last_lt > len(text) - 8:
                        self._buffer = text[last_lt:]
                    # Discard everything (we're inside think tag)
                    i = len(text)

        return "".join(output)

    def flush(self) -> str:
        """Flush any remaining buffered content.

        Call this when the stream ends to get any remaining content.

        Returns:
            Any remaining content that was buffered
        """
        result = ""
        if self._state == _State.NORMAL and self._buffer:
            # Buffer contains content that wasn't a tag
            result = self._buffer
        self._buffer = ""
        self._state = _State.NORMAL
        self._depth = 0
        return result

    def reset(self) -> None:
        """Reset filter state for reuse."""
        self._state = _State.NORMAL
        self._buffer = ""
        self._depth = 0


def filter_think_tags(text: str) -> str:
    """One-shot filter for complete text.

    Convenience function for non-streaming use.

    Args:
        text: Complete text to filter

    Returns:
        Text with think tags removed
    """
    f = ThinkTagFilter()
    result = f.feed(text)
    result += f.flush()
    return result
