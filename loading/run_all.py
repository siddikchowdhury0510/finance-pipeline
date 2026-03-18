from load_stocks import load_stocks_to_bigquery
from load_crypto import load_crypto_to_bigquery
from load_macro import load_macro_to_bigquery

if __name__ == '__main__':
    print("Starting loading pipeline...\n")
    
    print("--- Loading Stocks ---")
    load_stocks_to_bigquery()
    
    print("\n--- Loading Crypto ---")
    load_crypto_to_bigquery()
    
    print("\n--- Loading Macro ---")
    load_macro_to_bigquery()
    
    print("\nLoading complete!")
