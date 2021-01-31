"""
jsonb_parser -- parser entry point.
"""

# Copyright (C) 2021 Daniele Varrazzo

import struct
from typing import Any, Callable, cast, Dict, List, Tuple, Union

Buffer = Union[bytes, bytearray, memoryview]
JArray = List[Any]
JObject = Dict[str, Any]
JString = str
JNumeric = Union[int, float]  # TODO decimal too?
JBool = bool
JNull = type(None)
JContainer = Union[JArray, JObject]
JScalar = Union[JNull, JBool, JNumeric, JString]


def parse_jsonb(data: Buffer) -> Any:
    v = JsonbParseVisitor(data)
    v.visit()
    return v.object


class JsonbParseVisitor:
    def __init__(self, data: Buffer):
        self.data = data
        self._object: Any = None
        self._parsed = False
        self._pos = 0

    def visit(self) -> None:
        """Parse the input data.

        The result will be found in `self.object`.
        """
        val = self._get32()
        self._object = self._parse_container(val)
        if jc_is_scalar(val):
            self._object = cast(JArray, self._object)[0]
        self._parsed = True

    @property
    def object(self) -> Any:
        """The object parsed."""
        if not self._parsed:
            raise ValueError("no data parsed yet")

        return self._object

    def _parse_entry(self) -> Any:
        val = self._get32()
        typ = jbe_type(val)
        if typ == JENTRY_ISNULL:
            return None
        elif typ == JENTRY_ISBOOL_TRUE:
            return True
        elif typ == JENTRY_ISBOOL_FALSE:
            return False
        elif typ == JENTRY_ISSTRING:
            return self._parse_string(val)
        elif typ == JENTRY_ISNUMERIC:
            return self._parse_numeric(val)
        elif typ == JENTRY_ISCONTAINER:
            return self._parse_container(val)
        else:
            raise ValueError(f"bad entry header: {val}")

    def _parse_container(self, head: int) -> JContainer:
        if jc_is_array(head):
            return self._parse_array(head)
        elif jc_is_object(head):
            return self._parse_object(head)
        else:
            raise ValueError(f"bad container header: {head}")

    def _parse_array(self, head: int) -> JArray:
        size = jc_size(head)
        res = []
        for i in range(size):
            res.append(self._parse_entry())
        return res

    def _parse_object(self, head: int) -> JObject:
        raise NotImplementedError("object parsing")

    def _parse_string(self, head: int) -> JString:
        raise NotImplementedError("string parsing")

    def _parse_numeric(self, head: int) -> JNumeric:
        raise NotImplementedError("numeric parsing")

    def _get32(self) -> int:
        """Parse an uint32 from the current position.

        Advance the current position after parsing.
        """
        val = self._get32_at(self._pos)
        self._pos += 4
        return val

    def _get32_at(self, pos: int) -> int:
        """Parse an uint32 from an arbitrary position.

        *Don't* advance the current position after parsing.
        """
        return _unpack_uint4(self.data, pos)[0]


def is_container(val: int) -> bool:
    """True if a JEntry header represents a container (array or object)."""
    return (val & JENTRY_TYPEMASK) == JENTRY_ISCONTAINER


# JsonEntry parsing
JENTRY_OFFLENMASK = 0x0FFFFFFF
JENTRY_TYPEMASK = 0x70000000
JENTRY_HAS_OFF = 0x80000000

# values stored in the type bits
JENTRY_ISSTRING = 0x00000000
JENTRY_ISNUMERIC = 0x10000000
JENTRY_ISBOOL_FALSE = 0x20000000
JENTRY_ISBOOL_TRUE = 0x30000000
JENTRY_ISNULL = 0x40000000
JENTRY_ISCONTAINER = 0x50000000  # array or object


def jbe_offlenfld(je: int) -> int:
    return je & JENTRY_OFFLENMASK


def jbe_has_off(je: int) -> bool:
    return (je & JENTRY_HAS_OFF) != 0


def jbe_isstring(je: int) -> bool:
    return (je & JENTRY_TYPEMASK) == JENTRY_ISSTRING


def jbe_isnumeric(je: int) -> bool:
    return (je & JENTRY_TYPEMASK) == JENTRY_ISNUMERIC


def jbe_iscontainer(je: int) -> bool:
    return (je & JENTRY_TYPEMASK) == JENTRY_ISCONTAINER


def jbe_isnull(je: int) -> bool:
    return je & JENTRY_TYPEMASK == JENTRY_ISNULL


def jbe_isbool_true(je: int) -> bool:
    return (je & JENTRY_TYPEMASK) == JENTRY_ISBOOL_TRUE


def jbe_isbool_false(je: int) -> bool:
    return (je & JENTRY_TYPEMASK) == JENTRY_ISBOOL_FALSE


def jbe_isbool(je: int) -> bool:
    return jbe_isbool_true(je) or jbe_isbool_false(je)


def jbe_type(je: int) -> int:
    return je & JENTRY_TYPEMASK


# flags for the header-field in JsonbContainer
JB_CMASK = 0x0FFFFFFF  # mask for count field
JB_FSCALAR = 0x10000000  # flag bits
JB_FOBJECT = 0x20000000
JB_FARRAY = 0x40000000


def jc_size(val: int) -> int:
    """Return the size a JsonContainer"""
    return val & JB_CMASK


def jc_is_scalar(val: int) -> bool:
    """Return True if a JsonContainer header represents a scalar"""
    return val & JB_FSCALAR != 0


def jc_is_object(val: int) -> bool:
    """Return True if a JsonContainer header represents an object"""
    return val & JB_FOBJECT != 0


def jc_is_array(val: int) -> bool:
    """Return True if a JsonContainer header represents an array"""
    return val & JB_FARRAY != 0


_UnpackInt = Callable[[Buffer, int], Tuple[int]]

# TODO: the server might be big-endian. Detect from first bytes?
_unpack_uint4 = cast(_UnpackInt, struct.Struct("<I").unpack_from)
