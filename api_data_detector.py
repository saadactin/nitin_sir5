"""
Automatic Data Path Detection for API Responses
Intelligently detects where the actual data records are in a JSON response
"""
import json
import logging
from typing import Any, Optional, Tuple, List

logger = logging.getLogger(__name__)


def detect_data_path(json_response: Any) -> Tuple[Optional[str], List[Any]]:
    """
    Automatically detect the path to the data array in a JSON response.
    
    Returns:
        Tuple of (data_path, records):
        - data_path: The path to the data (e.g., "data", "results", "data.items")
        - records: The actual list of records found
        
    Examples:
        {"data": [...]} -> ("data", [...])
        {"results": [...]} -> ("results", [...])
        {"data": {"items": [...]}} -> ("data.items", [...])
        [{...}, {...}] -> ("", [{...}, {...}])  # Already an array
    """
    
    # Case 1: Response is already an array
    if isinstance(json_response, list):
        if len(json_response) > 0:
            logger.info(f"✅ Response is already an array with {len(json_response)} records")
            return ("", json_response)
        else:
            logger.warning("⚠️  Response is an empty array")
            return ("", [])
    
    # Case 2: Response is an object - search for arrays within it
    if isinstance(json_response, dict):
        return _find_data_array_in_dict(json_response, "")
    
    # Case 3: Not a valid JSON structure
    logger.error(f"❌ Invalid JSON structure: {type(json_response)}")
    return (None, [])


def _find_data_array_in_dict(data: dict, current_path: str = "") -> Tuple[Optional[str], List[Any]]:
    """
    Recursively search for arrays in a dictionary, prioritizing common data keys.
    """
    
    # Priority order: keys that commonly contain data arrays
    priority_keys = ["data", "results", "items", "records", "rows", "list", "content", "response"]
    
    # First pass: Check priority keys
    for key in priority_keys:
        if key in data:
            value = data[key]
            new_path = f"{current_path}.{key}" if current_path else key
            
            # If it's an array, we found it!
            if isinstance(value, list):
                if len(value) > 0:
                    logger.info(f"✅ Found data array at path '{new_path}' with {len(value)} records")
                    return (new_path, value)
                else:
                    logger.warning(f"⚠️  Found empty array at path '{new_path}'")
                    return (new_path, [])
            
            # If it's a dict, recurse into it
            elif isinstance(value, dict):
                result_path, result_records = _find_data_array_in_dict(value, new_path)
                if result_records:
                    return (result_path, result_records)
    
    # Second pass: Check all other keys
    for key, value in data.items():
        if key in priority_keys:
            continue  # Already checked
        
        new_path = f"{current_path}.{key}" if current_path else key
        
        # If it's an array, we found it!
        if isinstance(value, list):
            if len(value) > 0:
                # Check if this array contains objects (not just primitives)
                if isinstance(value[0], dict):
                    logger.info(f"✅ Found data array at path '{new_path}' with {len(value)} records")
                    return (new_path, value)
                else:
                    logger.debug(f"Skipping array of primitives at '{new_path}'")
            else:
                logger.warning(f"⚠️  Found empty array at path '{new_path}'")
        
        # If it's a dict, recurse into it (only 1 level deep to avoid complex structures)
        elif isinstance(value, dict) and not current_path:
            result_path, result_records = _find_data_array_in_dict(value, new_path)
            if result_records:
                return (result_path, result_records)
    
    # No array found
    logger.warning(f"⚠️  No data array found in response")
    return (None, [])


def auto_detect_and_extract(json_response: Any, user_provided_path: Optional[str] = None) -> Tuple[str, List[Any]]:
    """
    Extract data from JSON response, using user-provided path if available,
    otherwise automatically detecting it.
    
    Args:
        json_response: The JSON response from the API
        user_provided_path: Optional path provided by the user (e.g., "data", "results.items")
        
    Returns:
        Tuple of (final_path, records)
    """
    
    # If user provided a path, try to use it first
    if user_provided_path and user_provided_path.strip():
        logger.info(f"🔍 Using user-provided data path: '{user_provided_path}'")
        
        try:
            records = json_response
            for key in user_provided_path.split('.'):
                if isinstance(records, dict) and key in records:
                    records = records[key]
                else:
                    logger.error(f"❌ User-provided path '{user_provided_path}' not found in response")
                    logger.info("🤖 Falling back to automatic detection...")
                    return detect_data_path(json_response)
            
            # Ensure records is a list
            if not isinstance(records, list):
                records = [records]
            
            logger.info(f"✅ Extracted {len(records)} records using user-provided path")
            return (user_provided_path, records)
            
        except Exception as e:
            logger.error(f"❌ Error using user-provided path: {e}")
            logger.info("🤖 Falling back to automatic detection...")
    
    # Auto-detect the path
    logger.info("🤖 Auto-detecting data path in API response...")
    detected_path, records = detect_data_path(json_response)
    
    if detected_path is not None:
        return (detected_path, records)
    else:
        # Last resort: treat the entire response as a single record
        logger.warning("⚠️  Could not find data array, treating entire response as single record")
        if isinstance(json_response, dict):
            return ("", [json_response])
        else:
            return ("", [])


def print_response_structure(json_response: Any, indent: int = 0) -> None:
    """
    Helper function to print the structure of a JSON response (for debugging)
    """
    prefix = "  " * indent
    
    if isinstance(json_response, dict):
        print(f"{prefix}{{")
        for key, value in json_response.items():
            if isinstance(value, list):
                print(f"{prefix}  '{key}': [{len(value)} items]")
                if len(value) > 0:
                    print(f"{prefix}    Sample item:")
                    print_response_structure(value[0], indent + 2)
            elif isinstance(value, dict):
                print(f"{prefix}  '{key}': {{...}}")
            else:
                print(f"{prefix}  '{key}': {type(value).__name__}")
        print(f"{prefix}}}")
    elif isinstance(json_response, list):
        print(f"{prefix}[{len(json_response)} items]")
        if len(json_response) > 0:
            print(f"{prefix}  Sample item:")
            print_response_structure(json_response[0], indent + 1)
    else:
        print(f"{prefix}{type(json_response).__name__}: {str(json_response)[:50]}")


# Example usage and tests
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test Case 1: Response with "data" key
    test1 = {
        "data": [
            {"id": 1, "name": "Test 1"},
            {"id": 2, "name": "Test 2"}
        ],
        "info": {"count": 2}
    }
    print("\n=== Test 1: Standard 'data' key ===")
    print_response_structure(test1)
    path, records = auto_detect_and_extract(test1)
    print(f"Result: path='{path}', records={len(records)}")
    
    # Test Case 2: Array at root
    test2 = [
        {"id": 1, "name": "Test 1"},
        {"id": 2, "name": "Test 2"}
    ]
    print("\n=== Test 2: Array at root ===")
    path, records = auto_detect_and_extract(test2)
    print(f"Result: path='{path}', records={len(records)}")
    
    # Test Case 3: Nested data
    test3 = {
        "response": {
            "results": [
                {"id": 1, "name": "Test 1"},
                {"id": 2, "name": "Test 2"}
            ]
        }
    }
    print("\n=== Test 3: Nested 'response.results' ===")
    print_response_structure(test3)
    path, records = auto_detect_and_extract(test3)
    print(f"Result: path='{path}', records={len(records)}")
    
    # Test Case 4: With user-provided path
    print("\n=== Test 4: User-provided path 'data' ===")
    path, records = auto_detect_and_extract(test1, "data")
    print(f"Result: path='{path}', records={len(records)}")

