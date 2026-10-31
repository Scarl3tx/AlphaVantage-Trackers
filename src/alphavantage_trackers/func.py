import json
import sys
def load_json_file(file_path):
    """Safely loads a JSON file or halts execution if missing."""
    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"❌ Critical Failure: File not found at '{file_path}'")
        sys.exit(1) # Stop the script immediately instead of crashing later