"""SOAP client using create_soap_client() and soap_call().

Demonstrates:
    - create_soap_client() — creating a Zeep client via Request
    - soap_call() — calling a SOAP service method with error handling
    - should_abort() works after SOAP errors just like after HTTP errors
    - Error tracking in ExceptionDescriptor for SOAP faults

This example uses a public SOAP test service (DNEOnline calculator).

Run:
    python -m data_collector.examples.request.07_soap_client
"""


import logging

from data_collector.utilities.request import Request

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CALCULATOR_WSDL = "http://www.dneonline.com/calculator.asmx?WSDL"


def main() -> None:
    """Run SOAP client creation and request examples."""
    req = Request(timeout=15, retries=2)

    # --- Create SOAP client ---
    print("=== Creating SOAP client ===")
    try:
        client = req.create_soap_client(CALCULATOR_WSDL)
        print(f"Client created: {type(client).__name__}")
    except ImportError as exc:
        print(f"Zeep not installed: {exc}")
        print("Install with: pip install data_collector[soap]")
        return

    # --- Call SOAP methods ---
    print("\n=== SOAP calls ===")
    result = req.soap_call(client.service.Add, intA=10, intB=25)
    print(f"Add(10, 25) = {result}")

    result = req.soap_call(client.service.Multiply, intA=7, intB=6)
    print(f"Multiply(7, 6) = {result}")

    result = req.soap_call(client.service.Divide, intA=100, intB=4)
    print(f"Divide(100, 4) = {result}")

    # --- Request count and error tracking ---
    print("\n=== Statistics ===")
    print(f"Request count: {req.request_count}")
    print(f"Errors: {req.exception_descriptor.errors}")

    # --- should_abort() after SOAP call ---
    print(f"\nshould_abort: {req.should_abort(logger)}")


if __name__ == "__main__":
    main()
