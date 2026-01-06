import json

# This script simulates the AI's output so you can work on the UI
# without waiting for Google to unblock your API key.

print("⚙️ Generating mock Semantic Map programmatically...")

# These are typical values found in the BIRD Financial/District dataset
# We map "Dirty/Abbreviated" values to "Clean/Canonical" values.
mock_data = {
    "NY": "New York",
    "N.Y.": "New York",
    "NYC": "New York",
    "Calif": "California",
    "CA": "California",
    "L.A.": "Los Angeles",
    "LA": "Los Angeles",
    "SF": "San Francisco",
    "San Fran": "San Francisco",
    "W.D.C": "Washington",
    "D.C.": "Washington",
    "Tex": "Texas",
    "TX": "Texas",
    "Bklyn": "Brooklyn",
    "Manh": "Manhattan",
}

# Save this to the JSON file
output_file = "semantic_map.json"
with open(output_file, "w") as f:
    json.dump(mock_data, f, indent=4)

print(f"✅ Success! Created '{output_file}' with {len(mock_data)} entries.")
print("🚀 You can now run 'streamlit run step3_app.py'")
