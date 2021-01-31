"""
jsonb_parser -- parser entry point.
"""

# Copyright (C) 2021 Daniele Varrazzo

import struct
import codecs
from typing import Any, Callable, cast, Dict, List, Tuple, Union
from collections import namedtuple

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
        self._object = self._parse_root()
        self._parsed = True

    @property
    def object(self) -> Any:
        """The object parsed."""
        if not self._parsed:
            raise ValueError("no data parsed yet")

        return self._object

    def _parse_root(self) -> Any:
        jc = self._get32_at(0)
        if jc_is_array(jc):
            rv = self._parse_array(jc, 0)
            return rv[0] if jc_is_scalar(jc) else rv
        elif jc_is_object(jc):
            return self._parse_object(jc, 0)
        else:
            raise ValueError(f"bad root header: {jc}")

    def _parse_entry(self, je: int, pos: int) -> Any:
        typ = jbe_type(je)
        if typ == JENTRY_ISNULL:
            return None
        elif typ == JENTRY_ISBOOL_TRUE:
            return True
        elif typ == JENTRY_ISBOOL_FALSE:
            return False
        elif typ == JENTRY_ISSTRING:
            return self._parse_string(je, pos)
        elif typ == JENTRY_ISNUMERIC:
            return self._parse_numeric(je, pos)
        elif typ == JENTRY_ISCONTAINER:
            return self._parse_container(je, pos)
        else:
            raise ValueError(f"bad entry header: {je}")

    def _parse_container(self, je: int, pos: int) -> JContainer:
        wpad = pos % 4  # would you like some padding?
        if wpad:
            pos += 4 - wpad
        jc = self._get32_at(pos)
        if jc_is_array(jc):
            return self._parse_array(jc, pos)
        elif jc_is_object(jc):
            return self._parse_object(jc, pos)
        else:
            raise ValueError(f"bad container header: {jc}")

    def _parse_array(self, jc: int, pos: int) -> JArray:
        size = jc_size(jc)
        if not size:
            return []

        res = []
        pos += 4  # past the container head
        valpos = pos + 4 * size  # where are the values, past the jentries
        for i in range(size):
            je = self._get32_at(pos + 4 * i)
            obj = self._parse_entry(je, valpos)
            res.append(obj)
            valpos += jbe_offlenfld(je)

        return res

    def _parse_object(self, jc: int, pos: int) -> JObject:
        raise NotImplementedError("object parsing")

    def _parse_string(self, je: int, pos: int) -> JString:
        length = jbe_offlenfld(je)
        return _decode_utf8(self.data[pos : pos + length])[0]

    def _parse_numeric(self, je: int, pos: int) -> JNumeric:
        raise NotImplementedError("numeric parsing")

    def _get32(self) -> int:
        """Parse an uint32 from the current position.

        Advance the current position after parsing.
        """
        val = _unpack_uint4(self.data, self._pos)[0]
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


JEDetails = namedtuple("JEDetails", "type offlen hasoff")


def parse_je(je: int) -> JEDetails:
    """Debug helper to check what's in a JEntry"""
    typ = {
        JENTRY_ISSTRING: "str",
        JENTRY_ISNUMERIC: "num",
        JENTRY_ISCONTAINER: "cont",
        JENTRY_ISNULL: "null",
        JENTRY_ISBOOL_TRUE: "true",
        JENTRY_ISBOOL_FALSE: "false",
    }[jbe_type(je)]
    return JEDetails(typ, jbe_offlenfld(je), jbe_has_off(je))


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


JCDetails = namedtuple("JCDetails", "type size scal")


def parse_jc(jc: int) -> JCDetails:
    """Debug helper to check what's in a JsonContainer"""
    if jc_is_array(jc):
        typ = "array"
    if jc_is_object(jc):
        typ = "object"
    return JCDetails(typ, jc_size(jc), jc_is_scalar(jc))


_UnpackInt = Callable[[Buffer, int], Tuple[int]]

# TODO: the server might be big-endian. Detect from first bytes?
_unpack_uint4 = cast(_UnpackInt, struct.Struct("<I").unpack_from)

_decode_utf8 = codecs.lookup("utf8").decode
