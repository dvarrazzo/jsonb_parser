"""
jsonb_parser -- jsonb on-disk format parser.
"""

# Copyright (C) 2021 Daniele Varrazzo

import struct
import codecs
from typing import Any, Callable, cast, Dict, List, Tuple, Union
from collections import namedtuple

from .numeric import parse_numeric

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
    v = JsonbParser(data)
    v.parse()
    return v.object


class JsonbParser:
    """
    An object to parse a buffer containing a jsonb data.
    """

    def __init__(self, data: Buffer):
        self.data = data
        self._object: Any = None
        self._parsed = False

    def parse(self) -> None:
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
        """Parse and return root element of the data.

        The root element is always a container. If the json is a scalar, it is
        represented as a 1-elem array, with the "scalar" bit set.
        """
        jc = self._get32(0)
        if jc_is_array(jc):
            rv = self._parse_array(jc, 0)
            return rv[0] if jc_is_scalar(jc) else rv
        elif jc_is_object(jc):
            return self._parse_object(jc, 0)
        else:
            raise ValueError(f"bad root header: 0x{jc:08x}")

    def _parse_container(self, je: int, pos: int) -> JContainer:
        """Parse and return a container found at pos in the data.

        A container is composed by a 4-aligned JsonContainer header with its
        type and length, followed by a number of JsonEntries, then the data for
        the variable-length entries (strings, numbers, other containers).

        Every entry describes the type of the value and either its length or
        the offset of its end from the start of the values area (the reason is
        that, in order to look up an element, storing only lengths has o(n)
        behaviour, storing only offset has o(1) behaviour but is harder to
        compress). Currently the server stores one offset each stride of 32
        items, but the client doesn't make any assumption about it.
        """
        pos += int32_pad[pos & 3]  # would you like some padding?
        jc = self._get32(pos)
        if jc_is_array(jc):
            return self._parse_array(jc, pos)
        elif jc_is_object(jc):
            return self._parse_object(jc, pos)
        else:
            raise ValueError(f"bad container header: 0x{jc:08x}")

    def _parse_array(self, jc: int, pos: int) -> JArray:
        """Parse an array and return it as a Python list.

        An array is a container with a sequence of JEntry representing its
        elements in the order they appear.
        """
        size = jc_size(jc)
        if not size:
            return []

        res = []
        pos += 4  # past the container head
        vstart = pos + 4 * size  # where are the values, past the jentries
        voff = 0
        for i in range(size):
            je = self._get32(pos + 4 * i)

            # calculate the value length
            # if has_off, flen is the offset from vstart, not the length
            flen = jbe_offlenfld(je)
            if jbe_has_off(je):
                flen -= voff

            obj = self._parse_entry(je, vstart + voff, flen)
            res.append(obj)
            voff += flen

        return res

    def _parse_object(self, jc: int, pos: int) -> JObject:
        """Parse an object and return it as a Python dict.

        An object is represented as a container with 2 * size JEntries. The
        first half are the keys, ordered in quasi-lexicographical order (first
        by length, then by content), the second half are the values, in the
        same order of the keys.
        """
        size = jc_size(jc)
        if not size:
            return {}

        res = []
        pos += 4  # past the container head
        vstart = pos + 4 * size * 2  # where are the values, past the jentries
        voff = 0
        for i in range(size * 2):
            je = self._get32(pos + 4 * i)

            # calculate the value length
            # if has_off, flen is the offset from vstart, not the length
            flen = jbe_offlenfld(je)
            if jbe_has_off(je):
                flen -= voff

            obj = self._parse_entry(je, vstart + voff, flen)
            res.append(obj)
            voff += flen

        return dict(zip(res[:size], res[size:]))

    def _parse_entry(self, je: int, pos: int, length: int) -> Any:
        """Parse a JsonEntry into a Python value."""
        typ = jbe_type(je)
        if typ == JENTRY_ISSTRING:
            return self._parse_string(pos, length)
        elif typ == JENTRY_ISNUMERIC:
            return self._parse_numeric(pos, length)
        elif typ == JENTRY_ISCONTAINER:
            return self._parse_container(je, pos)
        elif typ == JENTRY_ISNULL:
            return None
        elif typ == JENTRY_ISBOOL_TRUE:
            return True
        elif typ == JENTRY_ISBOOL_FALSE:
            return False
        else:
            raise ValueError(f"bad entry header: 0x{je:08x}")

    def _parse_string(self, pos: int, length: int) -> JString:
        """Parse a chunk of data into a Python string.

        JSON strings are utf-8. Note that we don't use the method `.decode()`
        here in order to support the memoryview object, which is more efficient
        than bytes/bytearray as it doesn't require a copy to be sliced.
        """
        return _decode_utf8(self.data[pos : pos + length])[0]

    def _parse_numeric(self, pos: int, length: int) -> JNumeric:
        """Parse a chunk of data into a Python numeric value.

        Note: this is a parser for the on-disk format, not the send/recv
        format. As such it is machine-dependent and probably incomplete.
        """
        # the format includes the varlena header and alignment padding
        off = 4 + int32_pad[pos & 3]
        return parse_numeric(self.data[pos + off : pos + length])

    def _get32(self, pos: int) -> int:
        """Parse an uint32 from a position in the buffer.

        Note: parsing little endian here. I assume the bytes order depends on
        the server machine architecture.

        TODO: Sniff it from the root container.
        """
        return _unpack_uint4(self.data, pos)[0]


# The following definitions are converted from Postgres source, and allow
# bit-level access to the JsonEntry and JsonContainer values. See
# https://github.com/postgres/postgres/blob/master/src/include/utils/jsonb.h
# for all the details.


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


def dis_je(je: int) -> JEDetails:
    """Debug helper to check what's in a JsonEntry."""
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
    """Return the size a JsonContainer."""
    return val & JB_CMASK


def jc_is_scalar(val: int) -> bool:
    """Return True if a JsonContainer header represents a scalar."""
    return val & JB_FSCALAR != 0


def jc_is_object(val: int) -> bool:
    """Return True if a JsonContainer header represents an object."""
    return val & JB_FOBJECT != 0


def jc_is_array(val: int) -> bool:
    """Return True if a JsonContainer header represents an array."""
    return val & JB_FARRAY != 0


JCDetails = namedtuple("JCDetails", "type size scal")


def dis_jc(jc: int) -> JCDetails:
    """Debug helper to check what's in a JsonContainer."""
    if jc_is_array(jc):
        typ = "array"
    if jc_is_object(jc):
        typ = "object"
    else:
        raise ValueError(f"not a container: 0x{jc:08x}")
    return JCDetails(typ, jc_size(jc), jc_is_scalar(jc))


_UnpackInt = Callable[[Buffer, int], Tuple[int]]

# TODO: the server might be big-endian. Detect from first bytes?
_unpack_uint4 = cast(_UnpackInt, struct.Struct("<I").unpack_from)

_decode_utf8 = codecs.lookup("utf8").decode

int32_pad = [0, 3, 2, 1]
