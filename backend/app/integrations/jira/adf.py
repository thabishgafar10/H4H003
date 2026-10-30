"""
Atlassian Document Format (ADF) Parser.
Recursively converts Jira Cloud v3 ADF JSON node trees into clean markdown/plain text
while preserving the structure of headings, lists, code blocks, and links.
"""

from typing import Any, Dict, List, Union


def adf_to_text(node: Union[Dict[str, Any], List[Any], str, None]) -> str:
    """
    Convert an ADF node or tree into markdown-formatted text.
    Handles None, raw strings, lists of nodes, and nested ADF structures.
    """
    if node is None:
        return ""
    if isinstance(node, str):
        return node.strip()
    if isinstance(node, list):
        return "".join(adf_to_text(item) for item in node).strip()
    if not isinstance(node, dict):
        return str(node)

    node_type = node.get("type", "")
    content = node.get("content", [])
    text = node.get("text", "")
    attrs = node.get("attrs", {})

    # Base text node
    if node_type == "text":
        marks = node.get("marks", [])
        formatted = text
        for mark in marks:
            m_type = mark.get("type")
            if m_type == "strong":
                formatted = f"**{formatted}**"
            elif m_type == "em":
                formatted = f"*{formatted}*"
            elif m_type == "code":
                formatted = f"`{formatted}`"
            elif m_type == "link":
                href = mark.get("attrs", {}).get("href", "")
                formatted = f"[{formatted}]({href})"
            elif m_type == "strike":
                formatted = f"~~{formatted}~~"
        return formatted

    # Containers and blocks
    if node_type == "doc":
        return "\n\n".join(filter(None, [adf_to_text(c) for c in content])).strip()

    elif node_type == "paragraph":
        return "".join(adf_to_text(c) for c in content)

    elif node_type == "heading":
        level = attrs.get("level", 1)
        prefix = "#" * max(1, min(6, level))
        inner = "".join(adf_to_text(c) for c in content)
        return f"{prefix} {inner}"

    elif node_type == "bulletList":
        items = [f"* {adf_to_text(c)}" for c in content]
        return "\n".join(items)

    elif node_type == "orderedList":
        order = attrs.get("order", 1)
        items = []
        for i, c in enumerate(content):
            items.append(f"{order + i}. {adf_to_text(c)}")
        return "\n".join(items)

    elif node_type == "listItem":
        return "".join(adf_to_text(c) for c in content).strip()

    elif node_type == "codeBlock":
        language = attrs.get("language", "")
        code_text = "".join(adf_to_text(c) for c in content)
        return f"```{language}\n{code_text}\n```"

    elif node_type == "blockquote":
        inner = "\n".join(adf_to_text(c) for c in content)
        lines = [f"> {line}" for line in inner.split("\n")]
        return "\n".join(lines)

    elif node_type == "hardBreak":
        return "\n"

    elif node_type == "rule":
        return "\n---\n"

    elif node_type == "inlineCard":
        url = attrs.get("url", "")
        return f"<{url}>"

    elif node_type == "mention":
        user_text = attrs.get("text", "") or attrs.get("id", "user")
        return f"@{user_text}"

    # Default fallback: recursively parse all children
    if content:
        return " ".join(filter(None, [adf_to_text(c) for c in content]))

    return text
