from kiteconnect import KiteConnect
import json

API_KEY = "your_api_key"
ACCESS_TOKEN = "your_fresh_access_token"

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# Fetch ALL instruments across exchanges
instruments = kite.instruments()

print(f"Total instruments fetched: {len(instruments)}")

# Save them to a JSON file
with open("all_instruments.json", "w") as f:
    json.dump(instruments, f, indent=2)

print("✅ Saved all instruments to all_instruments.json")