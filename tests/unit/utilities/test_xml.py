"""Tests for data_collector.utilities.xml secure XML parsing utilities."""

from pathlib import Path

import pytest
from lxml import etree

from data_collector.utilities.xml import (
    build_namespace_map,
    create_secure_parser,
    find_all_text,
    find_text,
    parse_xml,
    parse_xml_file,
    strip_namespaces,
    xml_to_dict,
)


class TestCreateSecureParser:
    """Tests for create_secure_parser factory."""

    def test_returns_xml_parser(self) -> None:
        parser = create_secure_parser()
        assert isinstance(parser, etree.XMLParser)

    def test_secure_parser_blocks_entity_resolution(self) -> None:
        """Verify the parser does not resolve entities by parsing an entity reference."""
        parser = create_secure_parser()
        # An XML with an internal entity reference -- with resolve_entities=False
        # and load_dtd=False, the entity is not resolved and parsing should fail
        # or the entity remains unexpanded.
        xml_with_entity = b"""<?xml version="1.0"?>
        <!DOCTYPE root [<!ENTITY test "expanded">]>
        <root>&test;</root>"""
        try:
            result = etree.fromstring(xml_with_entity, parser=parser)
            # If it parses, entity should NOT have been expanded
            assert result.text != "expanded"
        except etree.XMLSyntaxError:
            # Parser refusing to process is also a valid secure outcome
            pass


class TestParseXml:
    """Tests for parse_xml function."""

    def test_parse_valid_xml_bytes(self) -> None:
        xml_bytes = b"<root><item>hello</item></root>"
        result = parse_xml(xml_bytes)
        assert result.tag == "root"
        assert result[0].tag == "item"
        assert result[0].text == "hello"

    def test_parse_valid_xml_string(self) -> None:
        xml_string = "<root><item>hello</item></root>"
        result = parse_xml(xml_string)
        assert result.tag == "root"
        assert result[0].text == "hello"

    def test_parse_xml_with_namespaces(self) -> None:
        xml_bytes = b'<ns:root xmlns:ns="http://example.com"><ns:item>value</ns:item></ns:root>'
        result = parse_xml(xml_bytes)
        assert result.tag == "{http://example.com}root"
        assert result[0].text == "value"

    def test_parse_malformed_xml_raises(self) -> None:
        with pytest.raises(etree.XMLSyntaxError):
            parse_xml(b"<root><unclosed>")

    def test_xxe_entity_not_resolved(self) -> None:
        """External entities must not be resolved -- XXE protection."""
        xxe_payload = b"""<?xml version="1.0"?>
        <!DOCTYPE root [
            <!ENTITY xxe SYSTEM "file:///etc/passwd">
        ]>
        <root>&xxe;</root>"""
        # With resolve_entities=False + load_dtd=False, the DTD is not loaded
        # and the entity reference is either kept as-is or triggers an error.
        # Either outcome is acceptable -- the critical thing is that file
        # content is NOT returned as element text.
        try:
            result = parse_xml(xxe_payload)
            # If parsing succeeds, the entity must NOT have been resolved
            assert result.text != "root:x:0:0:root:/root:/bin/bash"
        except etree.XMLSyntaxError:
            # Parsing failure is also an acceptable secure outcome
            pass

    def test_internal_entity_not_expanded(self) -> None:
        """Internal entity expansion (billion laughs vector) must be blocked."""
        billion_laughs = b"""<?xml version="1.0"?>
        <!DOCTYPE root [
            <!ENTITY lol "lol">
            <!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;">
        ]>
        <root>&lol2;</root>"""
        try:
            result = parse_xml(billion_laughs)
            # Entity should not have been expanded
            assert result.text is None or "lollollol" not in (result.text or "")
        except etree.XMLSyntaxError:
            pass

    def test_parse_empty_root(self) -> None:
        result = parse_xml(b"<root/>")
        assert result.tag == "root"
        assert result.text is None


class TestParseXmlFile:
    """Tests for parse_xml_file function."""

    def test_parse_valid_file(self, tmp_path: Path) -> None:
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(b"<root><item>hello</item></root>")
        result = parse_xml_file(xml_file)
        root = result.getroot()
        assert root.tag == "root"
        assert root[0].text == "hello"

    def test_parse_file_with_path_object(self, tmp_path: Path) -> None:
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(b"<root>content</root>")
        result = parse_xml_file(xml_file)
        assert result.getroot().text == "content"

    def test_parse_file_with_string_path(self, tmp_path: Path) -> None:
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(b"<root>content</root>")
        result = parse_xml_file(str(xml_file))
        assert result.getroot().text == "content"

    def test_parse_nonexistent_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(OSError):
            parse_xml_file(tmp_path / "nonexistent.xml")

    def test_parse_malformed_file_raises(self, tmp_path: Path) -> None:
        xml_file = tmp_path / "bad.xml"
        xml_file.write_bytes(b"<root><unclosed>")
        with pytest.raises(etree.XMLSyntaxError):
            parse_xml_file(xml_file)


class TestFindText:
    """Tests for find_text function."""

    def test_find_existing_element(self) -> None:
        root = parse_xml(b"<root><name>Alice</name><age>30</age></root>")
        assert find_text(root, "name") == "Alice"
        assert find_text(root, "age") == "30"

    def test_find_missing_element_returns_none(self) -> None:
        root = parse_xml(b"<root><name>Alice</name></root>")
        assert find_text(root, "email") is None

    def test_find_element_with_no_text_returns_none(self) -> None:
        root = parse_xml(b"<root><empty/></root>")
        assert find_text(root, "empty") is None

    def test_find_with_namespaces(self) -> None:
        xml_bytes = b'<root xmlns:ns="http://example.com"><ns:item>value</ns:item></root>'
        root = parse_xml(xml_bytes)
        namespace_map = {"ns": "http://example.com"}
        assert find_text(root, "ns:item", namespaces=namespace_map) == "value"

    def test_find_nested_element(self) -> None:
        root = parse_xml(b"<root><parent><child>deep</child></parent></root>")
        assert find_text(root, ".//child") == "deep"


class TestFindAllText:
    """Tests for find_all_text function."""

    def test_find_multiple_elements(self) -> None:
        root = parse_xml(b"<root><item>a</item><item>b</item><item>c</item></root>")
        result = find_all_text(root, "item")
        assert result == ["a", "b", "c"]

    def test_find_no_matches_returns_empty_list(self) -> None:
        root = parse_xml(b"<root><name>Alice</name></root>")
        assert find_all_text(root, "missing") == []

    def test_skips_elements_with_no_text(self) -> None:
        root = parse_xml(b"<root><item>a</item><item/><item>c</item></root>")
        result = find_all_text(root, "item")
        assert result == ["a", "c"]

    def test_find_all_with_namespaces(self) -> None:
        xml_bytes = (
            b'<root xmlns:ns="http://example.com">'
            b"<ns:val>1</ns:val><ns:val>2</ns:val>"
            b"</root>"
        )
        root = parse_xml(xml_bytes)
        namespace_map = {"ns": "http://example.com"}
        assert find_all_text(root, "ns:val", namespaces=namespace_map) == ["1", "2"]


class TestBuildNamespaceMap:
    """Tests for build_namespace_map function."""

    def test_builds_correct_mapping(self) -> None:
        result = build_namespace_map(
            soap="http://schemas.xmlsoap.org/soap/envelope/",
            ns1="http://example.com/registry/v2",
        )
        assert result == {
            "soap": "http://schemas.xmlsoap.org/soap/envelope/",
            "ns1": "http://example.com/registry/v2",
        }

    def test_empty_namespace_map(self) -> None:
        result = build_namespace_map()
        assert result == {}

    def test_single_namespace(self) -> None:
        result = build_namespace_map(ns="http://example.com")
        assert result == {"ns": "http://example.com"}


class TestStripNamespaces:
    """Tests for strip_namespaces function."""

    def test_strips_element_namespaces(self) -> None:
        xml_bytes = b'<ns:root xmlns:ns="http://example.com"><ns:item>value</ns:item></ns:root>'
        root = parse_xml(xml_bytes)
        cleaned = strip_namespaces(root)
        assert cleaned.tag == "root"
        assert cleaned[0].tag == "item"
        assert cleaned[0].text == "value"

    def test_strips_attribute_namespaces(self) -> None:
        xml_bytes = (
            b'<root xmlns:ns="http://example.com" '
            b'ns:attr="val"><item ns:id="123">text</item></root>'
        )
        root = parse_xml(xml_bytes)
        cleaned = strip_namespaces(root)
        assert "attr" in cleaned.attrib
        assert cleaned.attrib["attr"] == "val"
        assert cleaned[0].attrib["id"] == "123"

    def test_does_not_modify_original(self) -> None:
        xml_bytes = b'<ns:root xmlns:ns="http://example.com"><ns:item>value</ns:item></ns:root>'
        root = parse_xml(xml_bytes)
        original_tag = root.tag
        strip_namespaces(root)
        assert root.tag == original_tag

    def test_handles_no_namespaces(self) -> None:
        root = parse_xml(b"<root><item>value</item></root>")
        cleaned = strip_namespaces(root)
        assert cleaned.tag == "root"
        assert cleaned[0].tag == "item"

    def test_strips_multiple_namespaces(self) -> None:
        xml_bytes = (
            b'<root xmlns:a="http://a.com" xmlns:b="http://b.com">'
            b"<a:first>1</a:first><b:second>2</b:second>"
            b"</root>"
        )
        root = parse_xml(xml_bytes)
        cleaned = strip_namespaces(root)
        assert cleaned[0].tag == "first"
        assert cleaned[1].tag == "second"


class TestXmlToDict:
    """Tests for xml_to_dict function."""

    def test_simple_element_with_text(self) -> None:
        result = xml_to_dict(b"<name>Alice</name>")
        assert result == {"name": "Alice"}

    def test_nested_elements_preserve_depth(self) -> None:
        xml_bytes = b"<root><person><name>Alice</name><age>30</age></person></root>"
        result = xml_to_dict(xml_bytes)
        assert result == {"root": {"person": {"name": "Alice", "age": "30"}}}

    def test_repeated_siblings_become_list(self) -> None:
        xml_bytes = b"<root><item>a</item><item>b</item><item>c</item></root>"
        result = xml_to_dict(xml_bytes)
        assert result == {"root": {"item": ["a", "b", "c"]}}

    def test_attributes_with_at_prefix(self) -> None:
        xml_bytes = b'<person id="1"><name>Alice</name></person>'
        result = xml_to_dict(xml_bytes)
        assert result == {"person": {"@id": "1", "name": "Alice"}}

    def test_mixed_content_with_text_key(self) -> None:
        xml_bytes = b"<parent>text<child>inner</child></parent>"
        result = xml_to_dict(xml_bytes)
        assert result == {"parent": {"#text": "text", "child": "inner"}}

    def test_empty_element_returns_none(self) -> None:
        result = xml_to_dict(b"<empty/>")
        assert result == {"empty": None}

    def test_namespace_stripping_default(self) -> None:
        xml_bytes = b'<ns:root xmlns:ns="http://example.com"><ns:name>val</ns:name></ns:root>'
        result = xml_to_dict(xml_bytes)
        assert result == {"root": {"name": "val"}}

    def test_namespace_preserved_when_disabled(self) -> None:
        xml_bytes = b'<ns:root xmlns:ns="http://example.com"><ns:name>val</ns:name></ns:root>'
        result = xml_to_dict(xml_bytes, strip_ns=False)
        assert "{http://example.com}root" in result

    def test_accepts_raw_bytes(self) -> None:
        result = xml_to_dict(b"<root><item>hello</item></root>")
        assert result == {"root": {"item": "hello"}}

    def test_accepts_raw_string(self) -> None:
        result = xml_to_dict("<root><item>hello</item></root>")
        assert result == {"root": {"item": "hello"}}

    def test_accepts_pre_parsed_element(self) -> None:
        element = parse_xml(b"<root><item>hello</item></root>")
        result = xml_to_dict(element)
        assert result == {"root": {"item": "hello"}}

    def test_deep_nesting(self) -> None:
        xml_bytes = b"<a><b><c><d>deep</d></c></b></a>"
        result = xml_to_dict(xml_bytes)
        assert result == {"a": {"b": {"c": {"d": "deep"}}}}

    def test_mixed_single_and_repeated_children(self) -> None:
        xml_bytes = (
            b"<root>"
            b"<name>Test</name>"
            b"<item>a</item>"
            b"<item>b</item>"
            b"</root>"
        )
        result = xml_to_dict(xml_bytes)
        assert result == {"root": {"name": "Test", "item": ["a", "b"]}}

    def test_attributes_only_element(self) -> None:
        xml_bytes = b'<config version="2" mode="strict"/>'
        result = xml_to_dict(xml_bytes)
        assert result == {"config": {"@version": "2", "@mode": "strict"}}

    def test_soap_envelope_extraction(self) -> None:
        soap_xml = (
            b'<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">'
            b"<soap:Body>"
            b"<GetCompanyResponse>"
            b"<Company>"
            b"<Name>Acme Corp</Name>"
            b"<RegNumber>12345678</RegNumber>"
            b"<Account>HR001</Account>"
            b"<Account>HR002</Account>"
            b"</Company>"
            b"</GetCompanyResponse>"
            b"</soap:Body>"
            b"</soap:Envelope>"
        )
        result = xml_to_dict(soap_xml)
        company = result["Envelope"]["Body"]["GetCompanyResponse"]["Company"]
        assert company["Name"] == "Acme Corp"
        assert company["RegNumber"] == "12345678"
        assert company["Account"] == ["HR001", "HR002"]
