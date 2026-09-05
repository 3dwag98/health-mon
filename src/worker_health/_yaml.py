"""A YAML subset loader, used only when PyYAML is not installed.

The package promises zero required dependencies, and a health SDK that
cannot start because a config parser is missing is a poor advertisement for
itself.  PyYAML is used when present; this is the fallback.

What it supports is exactly what a worker-health config file needs:

    mappings, nested by indentation
    lists of scalars and lists of mappings
    scalars: str, int, float, bool, null
    quoted strings, `#` comments, `---` document markers
    inline empty collections (`{}`, `[]`)

What it does NOT support, and rejects loudly rather than mis-parsing:
anchors, aliases, multi-line block scalars, flow mappings with content, and
multiple documents.  If a config file needs those, install PyYAML -- the
error message says so.
"""
from __future__ import annotations

import re
from typing import Any

__all__ = ["safe_load", "YamlSubsetError"]


class YamlSubsetError(ValueError):
    """Raised for YAML this loader deliberately refuses to guess at."""


_UNSUPPORTED = re.compile(r"(^|\s)(&\w+|\*\w+|<<:|\||>)(\s|$)")


def safe_load(text: str) -> Any:
    """Parse a YAML subset.  Uses PyYAML when it is installed."""
    try:
        import yaml            # type: ignore
    except Exception:
        pass
    else:
        return yaml.safe_load(text)

    lines = _significant(text)
    if not lines:
        return None
    value, index = _parse_block(lines, 0, lines[0][0])
    if index != len(lines):
        raise YamlSubsetError(
            f"line {lines[index][2]}: unexpected indentation; install PyYAML for "
            f"full YAML support"
        )
    return value


def _significant(text: str) -> list[tuple[int, str, int]]:
    """(indent, content, line number) for every line that carries meaning."""
    out: list[tuple[int, str, int]] = []
    for number, raw in enumerate(text.splitlines(), start=1):
        if "\t" in raw[: len(raw) - len(raw.lstrip())]:
            raise YamlSubsetError(f"line {number}: tabs cannot be used for indentation")
        stripped = _strip_comment(raw)
        if not stripped.strip() or stripped.strip() in ("---", "..."):
            continue
        if _UNSUPPORTED.search(stripped):
            raise YamlSubsetError(
                f"line {number}: this file uses YAML features the built-in loader "
                f"does not support (anchors, merges or block scalars). "
                f"Install PyYAML: pip install 'worker-health[yaml]'"
            )
        out.append((len(stripped) - len(stripped.lstrip()), stripped.strip(), number))
    return out


def _strip_comment(line: str) -> str:
    """Remove a trailing comment, respecting quotes."""
    quote = None
    for index, char in enumerate(line):
        if quote:
            if char == quote:
                quote = None
        elif char in "\"'":
            quote = char
        elif char == "#" and (index == 0 or line[index - 1] in " \t"):
            return line[:index]
    return line


def _parse_block(lines, index: int, indent: int):
    if index >= len(lines):
        return None, index
    content = lines[index][1]
    if content == "-" or content.startswith("- "):
        return _parse_list(lines, index, indent)
    return _parse_map(lines, index, indent)


def _parse_map(lines, index: int, indent: int):
    out: dict[str, Any] = {}
    while index < len(lines):
        line_indent, content, number = lines[index]
        if line_indent < indent:
            break
        if line_indent > indent:
            raise YamlSubsetError(f"line {number}: unexpected indentation")
        key, sep, inline = content.partition(":")
        if not sep:
            raise YamlSubsetError(f"line {number}: expected 'key: value'")
        key = _scalar(key.strip())
        inline = inline.strip()
        index += 1
        if inline:
            out[key] = _scalar(inline)
            continue
        # A nested block, or an explicitly empty value.
        if index < len(lines) and lines[index][0] > indent:
            out[key], index = _parse_block(lines, index, lines[index][0])
        elif index < len(lines) and lines[index][0] == indent and lines[index][1].startswith("-"):
            # A list may sit at the SAME indentation as its key, which is
            # both legal and the style most config files in the wild use.
            out[key], index = _parse_list(lines, index, indent)
        else:
            out[key] = None
    return out, index


def _parse_list(lines, index: int, indent: int):
    out: list[Any] = []
    while index < len(lines):
        line_indent, content, number = lines[index]
        if line_indent < indent or not (content == "-" or content.startswith("- ")):
            break
        if line_indent > indent:
            raise YamlSubsetError(f"line {number}: unexpected indentation in list")
        item = content[1:].strip()
        index += 1
        if not item:
            if index < len(lines) and lines[index][0] > indent:
                value, index = _parse_block(lines, index, lines[index][0])
                out.append(value)
            else:
                out.append(None)
            continue
        if ":" in item and not _is_quoted(item):
            # "- type: postgres" starts a mapping whose first key is on the
            # dash line.  Re-feed that key at the item's own indent.
            item_indent = line_indent + 2
            synthetic = [(item_indent, item, number)]
            while index < len(lines) and lines[index][0] > indent:
                synthetic.append(lines[index])
                index += 1
            value, _ = _parse_map(synthetic, 0, item_indent)
            out.append(value)
        else:
            out.append(_scalar(item))
    return out, index


def _is_quoted(text: str) -> bool:
    return len(text) >= 2 and text[0] == text[-1] and text[0] in "\"'"


def _scalar(text: str) -> Any:
    if _is_quoted(text):
        return text[1:-1]
    lowered = text.lower()
    if lowered in ("null", "~", ""):
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if text == "{}":
        return {}
    if text == "[]":
        return []
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        return [_scalar(part.strip()) for part in inner.split(",")] if inner else []
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        pass
    return text
