"""SOAP webservice integration using Zeep via the Request class.

Demonstrates:
    - LoggingService with debug=True (console only, no database sinks)
    - Request.create_soap_client() -- creating a Zeep client with inherited transport config
    - Request.soap_call() -- calling SOAP service methods with error handling
    - Zeep auto-deserialization -- complex nested SOAP responses as Python objects
    - zeep.helpers.serialize_object() -- converting Zeep objects to OrderedDict/list
    - ExceptionDescriptor error tracking after SOAP calls
    - should_abort() evaluation after SOAP errors

Uses the public CountryInfoService WSDL (oorsprong.org).
Returns nested country data: capitals, currencies, languages, continent groupings.

Run:
    python -m data_collector.examples.xml.01_soap_zeep.main
"""

import json
import logging
from typing import cast

from zeep.helpers import serialize_object  # type: ignore[reportUnknownVariableType]

from data_collector.settings.main import LogSettings
from data_collector.utilities.log.main import LoggingService
from data_collector.utilities.request import Request

COUNTRY_INFO_WSDL = "http://www.oorsprong.org/websamples.countryinfo/CountryInfoService.wso?WSDL"


def main() -> None:
    """Run SOAP proof-of-concept against the CountryInfoService."""
    log_settings = LogSettings(log_level=10)
    service = LoggingService("examples.xml.soap_zeep", settings=log_settings)
    service.debug = True
    logger = service.configure_logger()

    try:
        abort_logger = logging.getLogger("examples.xml.soap_zeep")
        request = Request(timeout=15, retries=2, backoff_factor=2)

        # --- Create SOAP client ---
        logger.info("Creating SOAP client", wsdl=COUNTRY_INFO_WSDL)
        soap_client = request.create_soap_client(COUNTRY_INFO_WSDL)
        logger.info("SOAP client created", client_type=type(soap_client).__name__)

        # --- CapitalCity: simple string return ---
        logger.info("=== CapitalCity (string output) ===")
        logger.info("INPUT", method="CapitalCity", sCountryISOCode="HR")
        result = request.soap_call(soap_client.service.CapitalCity, sCountryISOCode="HR")
        logger.info("OUTPUT", raw_type=type(result).__name__, output=result)

        if request.should_abort(abort_logger):
            return

        # --- FullCountryInfo: nested object with languages sub-list ---
        logger.info("=== FullCountryInfo (nested object) ===")
        logger.info("INPUT", method="FullCountryInfo", sCountryISOCode="HR")
        result = request.soap_call(soap_client.service.FullCountryInfo, sCountryISOCode="HR")
        serialized = cast(object, serialize_object(result))
        logger.info("OUTPUT", raw_type=type(result).__name__, output=json.dumps(serialized, default=str))

        if request.should_abort(abort_logger):
            return

        # --- CountriesUsingCurrency: list of objects ---
        logger.info("=== CountriesUsingCurrency (list of objects) ===")
        logger.info("INPUT", method="CountriesUsingCurrency", sISOCurrencyCode="EUR")
        result = request.soap_call(soap_client.service.CountriesUsingCurrency, sISOCurrencyCode="EUR")
        serialized = cast(object, serialize_object(result))
        logger.info("OUTPUT", raw_type=type(result).__name__, output=json.dumps(serialized, default=str))

        if request.should_abort(abort_logger):
            return

        # --- ListOfCurrenciesByName: large list ---
        logger.info("=== ListOfCurrenciesByName (large list) ===")
        logger.info("INPUT", method="ListOfCurrenciesByName")
        result = request.soap_call(soap_client.service.ListOfCurrenciesByName)
        serialized_list = cast(list[object], serialize_object(result))
        logger.info("OUTPUT", raw_type=type(result).__name__, total=len(serialized_list),
                     output=json.dumps(serialized_list[:5], default=str))

        # --- Request statistics ---
        logger.info(
            "Session statistics",
            total_requests=request.request_count,
            error_count=len(request.exception_descriptor.errors),
            should_abort=request.should_abort(abort_logger),
        )

    finally:
        service.stop()


if __name__ == "__main__":
    main()
