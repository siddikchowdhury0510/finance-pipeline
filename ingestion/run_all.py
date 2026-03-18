from tiingo_stocks import fetch_tiingo_stocks
from tiingo_crypto import fetch_tiingo_crypto
from fred_macro import fetch_fred_macro

if __name__ == '__main__':
    print("Starting ingestion pipeline...\n")
    
    print("--- Tiingo Stocks ---")
    fetch_tiingo_stocks()

    print("\n--- Tiingo Crypto ---")
    fetch_tiingo_crypto()

    print("\n--- FRED Macro ---")
    fetch_fred_macro()

    print("\nIngestion complete!")