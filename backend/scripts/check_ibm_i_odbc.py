"""
Check if IBM i Access ODBC driver is installed on this machine.
Run this after installing IBM i Access Client Solutions to verify.
"""
import sys

try:
    import pyodbc
except ImportError:
    print("pyodbc is not installed. Install with: pip install pyodbc")
    sys.exit(1)

drivers = pyodbc.drivers()
ibm_drivers = [d for d in drivers if "ibm" in d.lower() or "as400" in d.lower() or "iseries" in d.lower()]

if ibm_drivers:
    print("IBM i / AS400 ODBC driver(s) found:")
    for d in ibm_drivers:
        print(f"  - {d}")
    print("\nBackend can connect to AS400 for full load + auto-create schema.")
else:
    print("No IBM i / AS400 ODBC driver found.")
    print("Available drivers:", ", ".join(drivers))
    print("\nTo install: download IBM i Access Client Solutions from IBM,")
    print("  https://www.ibm.com/support/pages/ibm-i-access-client-solutions")
    print("  or https://www.ibm.com/resources/mrs/assets?source=swg-ia")
    print("  Then run the Windows installer (install_acs_64 for 64-bit).")
    sys.exit(1)
