"""Secure XML parsing utilities using lxml with hardened parser defaults.

lxml 6.x provides native XXE protection, network DTD blocking, and billion-laughs
mitigation at the libxml2 level. These utilities wrap lxml with a pre-configured
secure parser so that application code never uses an unprotected parser by accident.
"""

import copy
from pathlib import Path
from typing import Any

from lxml import etree


def create_secure_parser() -> etree.XMLParser:
    """Create an lxml XMLParser with security-hardened defaults.

    Returns:
        XMLParser configured to block XXE, network DTD retrieval, and entity expansion.

    Security properties:
        - ``resolve_entities=False``: prevents XML External Entity (XXE) injection
        - ``no_network=True``: blocks network access during DTD/schema resolution
        - ``dtd_validation=False``: skips DTD validation entirely
        - ``load_dtd=False``: does not load external DTD definitions
        - libxml2 built-in billion-laughs (quadratic blowup) protection is always active
    """
    return etree.XMLParser(
        resolve_entities=False,
        no_network=True,
        dtd_validation=False,
        load_dtd=False,
    )


def parse_xml(source: str | bytes) -> Any:
    """Parse an XML string or bytes into an element tree using the secure parser.

    Args:
        source: XML content as a string or bytes.

    Returns:
        Root element (``lxml.etree._Element``) of the parsed XML document.

    Raises:
        lxml.etree.XMLSyntaxError: If the XML is malformed.
    """
    parser = create_secure_parser()
    if isinstance(source, str):
        source = source.encode("utf-8")
    return etree.fromstring(source, parser=parser)


def parse_xml_file(file_path: str | Path) -> Any:
    """Parse an XML file into an element tree using the secure parser.

    Args:
        file_path: Path to the XML file.

    Returns:
        Parsed element tree (``lxml.etree._ElementTree``).

    Raises:
        lxml.etree.XMLSyntaxError: If the XML is malformed.
        OSError: If the file cannot be read.
    """
    parser = create_secure_parser()
    return etree.parse(str(file_path), parser=parser)


def find_text(
    element: Any,
    xpath: str,
    namespaces: dict[str, str] | None = None,
) -> str | None:
    """Extract text content from the first element matching an XPath expression.

    Args:
        element: lxml element (``lxml.etree._Element``) to search within.
        xpath: XPath expression to match.
        namespaces: Optional namespace prefix-to-URI mapping.

    Returns:
        Text content of the first matching element, or None if no match found
        or the matched element has no text.
    """
    result = element.find(xpath, namespaces=namespaces)
    if result is None:
        return None
    text: str | None = result.text
    return text


def find_all_text(
    element: Any,
    xpath: str,
    namespaces: dict[str, str] | None = None,
) -> list[str]:
    """Extract text content from all elements matching an XPath expression.

    Args:
        element: lxml element (``lxml.etree._Element``) to search within.
        xpath: XPath expression to match.
        namespaces: Optional namespace prefix-to-URI mapping.

    Returns:
        List of text content from matching elements. Elements with no text are skipped.
    """
    results: list[Any] = element.findall(xpath, namespaces=namespaces)
    return [str(r.text) for r in results if r.text is not None]


def build_namespace_map(**namespaces: str) -> dict[str, str]:
    """Build a namespace prefix-to-URI mapping from keyword arguments.

    Args:
        **namespaces: Keyword arguments where keys are namespace prefixes and
            values are namespace URIs.

    Returns:
        Dictionary mapping prefixes to URIs.

    Example::

        ns_map = build_namespace_map(
            soap="http://schemas.xmlsoap.org/soap/envelope/",
            ns1="http://example.com/registry/v2",
        )
    """
    return dict(namespaces)


def xml_to_dict(source: str | bytes | Any, strip_ns: bool = True) -> dict[str, Any]:
    """Convert XML into a nested Python dictionary preserving document depth.

    Accepts raw XML (str/bytes) or a pre-parsed lxml element. Raw input is parsed
    through the secure parser (XXE/DTD protection). Namespaces are stripped by
    default to produce clean dictionary keys suitable for ORM mapping.

    Args:
        source: Raw XML as string/bytes, or a pre-parsed lxml element.
        strip_ns: If True (default), namespace prefixes are removed from all
            element tags and attribute names before conversion.

    Returns:
        Nested dictionary where element tags are keys. Repeated sibling elements
        with the same tag become lists. Attributes are stored with ``@`` prefix.
        Text content of mixed elements is stored under ``#text``.

    Conversion rules::

        <name>Alice</name>              -> {"name": "Alice"}
        <item>a</item><item>b</item>    -> {"item": ["a", "b"]}
        <person id="1"><name>A</name>   -> {"person": {"@id": "1", "name": "A"}}
        <empty/>                         -> {"empty": None}

    Example::

        data = xml_to_dict(raw_xml_bytes)
        # data["Company"]["Name"] -> "Acme Corp"
        # data["Company"]["Accounts"]["Account"] -> [{"IBAN": "..."}, {"IBAN": "..."}]
    """
    element = parse_xml(source) if isinstance(source, (str, bytes)) else source

    if strip_ns:
        element = strip_namespaces(element)

    return _element_to_dict(element)


def _element_to_dict(element: Any) -> dict[str, Any]:
    """Recursively convert an lxml element to a nested dictionary."""
    result: dict[str, Any] = {}
    tag: str = str(element.tag)

    # Collect attributes with @ prefix
    for attribute_name, attribute_value in element.attrib.items():
        result[f"@{attribute_name}"] = attribute_value

    # Collect child elements
    children: dict[str, list[Any]] = {}
    for child in element:
        child_tag = str(child.tag)
        child_value = _element_to_dict(child)
        # Extract the inner value (unwrap the single-key dict)
        inner_value = child_value[child_tag]

        if child_tag not in children:
            children[child_tag] = []
        children[child_tag].append(inner_value)

    # Merge children into result
    for child_tag, values in children.items():
        if len(values) == 1:
            result[child_tag] = values[0]
        else:
            result[child_tag] = values

    # Handle text content
    text = element.text
    if text is not None:
        stripped_text = text.strip()
        if stripped_text:
            if result:
                # Mixed content: element has both children/attributes and text
                result["#text"] = stripped_text
            else:
                # Pure text element: return the dict with just the tag -> text
                return {tag: stripped_text}

    # If no children, no attributes, no text -> None
    if not result:
        return {tag: None}

    return {tag: result}


def strip_namespaces(element: Any) -> Any:
    """Remove all namespace prefixes from an element tree.

    Creates a deep copy of the element so the original is not modified. Namespace
    URIs are stripped from both element tags and attribute names.

    Args:
        element: lxml element (``lxml.etree._Element``) of the tree to strip.

    Returns:
        Deep copy of the element tree with all namespace prefixes removed.

    Example::

        # Before: {http://example.com}Root -> After: Root
        clean = strip_namespaces(root)
    """
    cleaned: Any = copy.deepcopy(element)
    for node in cleaned.iter():
        tag: str | bytes | None = node.tag
        if isinstance(tag, str) and "{" in tag:
            node.tag = tag.split("}", 1)[1]
        for attribute_name in list(node.attrib):
            attribute_key = str(attribute_name)
            if "{" in attribute_key:
                value = node.attrib.pop(attribute_name)
                clean_name = attribute_key.split("}", 1)[1]
                node.attrib[clean_name] = value
    return cleaned
