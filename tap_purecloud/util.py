import json


def safe_json_serialize_deserialize(data: any, **kwargs) -> dict:
    """
    Utility function that safely serializes unserializable fields in
    'data' as str and returns back the deserialized version

    Typically useful for serializing datetime.datetime or datetime.date
    Properly handle values if particular values for a key look incorrect
    """
    json_string = json.dumps(data, default=str, **kwargs)
    return json.loads(json_string)
