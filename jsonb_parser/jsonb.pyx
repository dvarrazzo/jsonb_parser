"""
jsonb_parser -- jsonb on-disk format parser.
"""

# Copyright (C) 2021 Daniele Varrazzo

from libc.stdint cimport uint32_t
from cpython.buffer cimport (
    PyObject_CheckBuffer, PyObject_GetBuffer, PyBUF_SIMPLE, PyBuffer_Release
)

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


cdef class JsonbParser:
    """
    An object to parse a buffer containing a jsonb data.
    """
    cdef readonly object data
    cdef object _object
    cdef int _parsed

    cdef int _gotbuf
    cdef Py_buffer _buf

    def __cinit__(self, data):
        self.data = data

        if PyObject_CheckBuffer(data):
            PyObject_GetBuffer(data, &(self._buf), PyBUF_SIMPLE)
            self._gotbuf = True
        else:
            raise TypeError(f"bytes or buffer expected, got {type(data)}")

        self._object = None
        self._parsed = 0
        self._gotbuf = 0

    def __dealloc__(self) -> None:
        if self._gotbuf:
            PyBuffer_Release(&(self._buf))

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

    cdef object _parse_root(self):
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

    cdef object _parse_container(self, uint32_t je, Py_ssize_t pos):
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
        wpad = pos % 4  # would you like some padding?
        if wpad:
            pos += 4 - wpad
        jc = self._get32(pos)
        if jc_is_array(jc):
            return self._parse_array(jc, pos)
        elif jc_is_object(jc):
            return self._parse_object(jc, pos)
        else:
            raise ValueError(f"bad container header: 0x{jc:08x}")

    cdef object _parse_array(self, uint32_t jc, Py_ssize_t pos):
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

    cdef object _parse_object(self, uint32_t jc, Py_ssize_t pos):
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

    cdef object _parse_entry(
        self, uint32_t je, Py_ssize_t pos, Py_ssize_t length
    ):
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

    cdef object _parse_string(self, uint32_t pos, Py_ssize_t length):
        """Parse a chunk of data into a Python string.

        JSON strings are utf-8. Note that we don't use the method `.decode()`
        here in order to support the memoryview object, which is more efficient
        than bytes/bytearray as it doesn't require a copy to be sliced.
        """
        return _decode_utf8(self.data[pos : pos + length])[0]

    cdef object _parse_numeric(self, uint32_t pos, Py_ssize_t length):
        """Parse a chunk of data into a Python numeric value.

        Note: this is a parser for the on-disk format, not the send/recv
        format. As such it is machine-dependent and probably incomplete.
        """
        # the format includes the varlena header and alignment padding
        off = 4
        wpad = pos % 4
        if wpad:
            off += 4 - wpad
        return parse_numeric(self.data[pos + off : pos + length])

    cdef uint32_t _get32(self, Py_ssize_t pos):
        """Parse an uint32 from a position in the data buffer.

        Note: parsing little endian here. I assume the bytes order depends on
        the server machine architecture.

        TODO: Sniff it from the root container.
        """
        if 0 <= pos <= self._buf.len - 4:
            return (<uint32_t *>(self._buf.buf + pos))[0]

        raise IndexError(f"can't access {pos}: buffer size is {self._buf.len}")


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

_decode_utf8 = codecs.lookup("utf8").decode
