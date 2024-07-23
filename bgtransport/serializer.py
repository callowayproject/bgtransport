"""Serializers for background transport messages."""

from json import JSONEncoder
from typing import Any, Callable, Optional, Union

Serializer = Callable[[Any], bytes]
"""Serializer type alias. A callable that takes a value and returns bytes."""


def json_serialize(
    obj: Any,
    *,
    skipkeys: bool = False,
    ensure_ascii: bool = True,
    check_circular: bool = True,
    allow_nan: bool = True,
    cls: Optional[type[JSONEncoder]] = None,
    indent: Union[int, str, None] = None,
    separators: Optional[tuple] = None,
    default: Optional[Callable] = None,
    sort_keys: bool = False,
    **kwargs,
) -> bytes:
    """Wrapper around `json.dumps` that converts the output to bytes."""
    from json import dumps

    result = dumps(
        obj,
        skipkeys=skipkeys,
        ensure_ascii=ensure_ascii,
        check_circular=check_circular,
        allow_nan=allow_nan,
        cls=cls,
        indent=indent,
        separators=separators,
        default=default,
        sort_keys=sort_keys,
        **kwargs,
    )
    return result.encode("utf-8")
