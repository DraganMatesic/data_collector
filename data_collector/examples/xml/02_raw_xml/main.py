"""Raw XML parsing and conversion using the xml utilities module.

Demonstrates:
    - LoggingService with debug=True (console only, no database sinks)
    - Request.get() to fetch raw XML from a public endpoint
    - xml_to_dict() -- convert raw XML to nested Python dictionary (namespace-stripped)
    - parse_xml() + find_text() -- XPath-based extraction
    - build_namespace_map() -- working with namespaced XML
    - strip_namespaces() -- removing namespace prefixes for cleaner access
    - Two approaches compared: dict traversal vs XPath navigation

Uses the ECB (European Central Bank) daily EUR exchange rate feed.
Returns real-time rates for 30+ currencies against EUR.

Run:
    python -m data_collector.examples.xml.02_raw_xml.main
"""

import json

from data_collector.settings.main import LogSettings
from data_collector.utilities.log.main import LoggingService
from data_collector.utilities.request import Request
from data_collector.utilities.xml import (
    build_namespace_map,
    find_text,
    parse_xml,
    strip_namespaces,
    xml_to_dict,
)

ECB_EXCHANGE_RATES_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"


def main() -> None:
    """Run raw XML proof-of-concept against the ECB exchange rate feed."""
    log_settings = LogSettings(log_level=10)
    service = LoggingService("examples.xml.raw_xml", settings=log_settings)
    service.debug = True
    logger = service.configure_logger()

    try:
        request = Request(timeout=15, retries=2)

        # --- Fetch raw XML ---
        logger.info("Fetching ECB exchange rates", url=ECB_EXCHANGE_RATES_URL)
        response = request.get(ECB_EXCHANGE_RATES_URL)

        if response is None or response.status_code != 200:
            logger.error("Failed to fetch exchange rates", status=getattr(response, "status_code", None))
            return

        raw_xml = response.content

        # --- INPUT: raw XML as received ---
        logger.info("INPUT raw XML", raw_xml=raw_xml.decode("utf-8"))

        # =====================================================================
        # Approach 1: xml_to_dict() -- full document to nested dict
        # =====================================================================
        logger.info("=== Approach 1: xml_to_dict (full document) ===")

        data = xml_to_dict(raw_xml)

        # --- OUTPUT: complete dict from xml_to_dict ---
        logger.info("OUTPUT xml_to_dict", output=json.dumps(data, default=str))

        # =====================================================================
        # Approach 2: parse_xml() + XPath -- targeted element extraction
        # =====================================================================
        logger.info("=== Approach 2: parse_xml + XPath (targeted extraction) ===")

        root = parse_xml(raw_xml)

        # ECB XML uses two namespaces
        namespace_map = build_namespace_map(
            gesmes="http://www.gesmes.org/xml/2002-08-01",
            ecb="http://www.ecb.int/vocabulary/2002-08-01/eurofxref",
        )

        # Extract sender name via namespaced XPath
        sender = find_text(root, ".//gesmes:name", namespaces=namespace_map)
        logger.info("XPath find_text result", xpath=".//gesmes:name", output=sender)

        # Strip namespaces for simpler XPath, then extract the date Cube sub-tree
        clean_root = strip_namespaces(root)
        date_element = clean_root.find(".//Cube[@time]")
        if date_element is not None:
            # --- OUTPUT: xml_to_dict on a sub-element (not full document) ---
            sub_tree_dict = xml_to_dict(date_element, strip_ns=False)
            logger.info(
                "OUTPUT xml_to_dict on sub-element",
                xpath=".//Cube[@time]",
                output=json.dumps(sub_tree_dict, default=str),
            )

        # --- Request statistics ---
        logger.info(
            "Session statistics",
            total_requests=request.request_count,
            error_count=len(request.exception_descriptor.errors),
        )

    finally:
        service.stop()


if __name__ == "__main__":
    main()
