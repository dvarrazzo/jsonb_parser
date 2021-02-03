"""
jsonb_parser -- jsonb on-disk format parser.
"""

# Copyright (C) 2021 Daniele Varrazzo

from libc.stdint cimport uint32_t
from cpython.buffer cimport (
    PyObject_CheckBuffer, PyObject_GetBuffer, PyBUF_SIMPLE, PyBuffer_Release
)
from cpython.unicode cimport PyUnicode_DecodeUTF8

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

ctypedef uint32_t JEntry
ctypedef uint32_t JCont

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

    cdef object _parse_container(self, JEntry je, Py_ssize_t pos):
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
        pos += int32_pad[pos % sizeof(uint32_t)]  # would you like some padding?
        cdef JCont jc = self._get32(pos)
        if jc_is_array(jc):
            return self._parse_array(jc, pos)
        elif jc_is_object(jc):
            return self._parse_object(jc, pos)
        else:
            raise ValueError(f"bad container header: 0x{jc:08x}")

    cdef object _parse_array(self, JCont jc, Py_ssize_t pos):
        """Parse an array and return it as a Python list.

        An array is a container with a sequence of JEntry representing its
        elements in the order they appear.
        """
        cdef Py_ssize_t size = jc_size(jc)
        if not size:
            return []

        cdef list res = []
        pos += sizeof(jc)  # past the container head
        # where are the values, past the jentries
        cdef Py_ssize_t vstart = pos + sizeof(JEntry) * size
        cdef Py_ssize_t voff = 0

        cdef JEntry je
        cdef Py_ssize_t flen
        cdef object obj
        for i in range(size):
            je = self._get32(pos + sizeof(JEntry) * i)

            # calculate the value length
            # if has_off, flen is the offset from vstart, not the length
            flen = jbe_offlenfld(je)
            if jbe_has_off(je):
                flen -= voff

            obj = self._parse_entry(je, vstart + voff, flen)
            res.append(obj)
            voff += flen

        return res

    cdef object _parse_object(self, JCont jc, Py_ssize_t pos):
        """Parse an object and return it as a Python dict.

        An object is represented as a container with 2 * size JEntries. The
        first half are the keys, ordered in quasi-lexicographical order (first
        by length, then by content), the second half are the values, in the
        same order of the keys.
        """
        cdef Py_ssize_t size = jc_size(jc)
        if not size:
            return {}

        cdef list res = []
        pos += 4  # past the container head
        # where are the values, past the jentries
        cdef Py_ssize_t vstart = pos + sizeof(JEntry) * size * 2
        cdef Py_ssize_t voff = 0

        cdef JEntry je
        cdef Py_ssize_t flen
        cdef object obj
        for i in range(size * 2):
            je = self._get32(pos + sizeof(JEntry) * i)

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
        self, JEntry je, Py_ssize_t pos, Py_ssize_t length
    ):
        """Parse a JsonEntry into a Python value."""
        if jbe_isstring(je):
            return self._parse_string(pos, length)
        elif jbe_isnumeric(je):
            return self._parse_numeric(pos, length)
        elif jbe_iscontainer(je):
            return self._parse_container(je, pos)
        elif jbe_isnull(je):
            return None
        elif jbe_isbool_true(je):
            return True
        elif jbe_isbool_false(je):
            return False
        else:
            raise ValueError(f"bad entry header: 0x{je:08x}")

    cdef object _parse_string(self, Py_ssize_t pos, Py_ssize_t length):
        """Parse a chunk of data into a Python string.

        JSON strings are utf-8. Note that we don't use the method `.decode()`
        here in order to support the memoryview object, which is more efficient
        than bytes/bytearray as it doesn't require a copy to be sliced.
        """
        if 0 <= pos <= self._buf.len - length:
            return PyUnicode_DecodeUTF8(
                <char *>(self._buf.buf + pos), length, NULL)

        raise IndexError(
            f"can't get {length} bytes from {pos}: buffer size is {self._buf.len}")

    cdef object _parse_numeric(self, Py_ssize_t pos, Py_ssize_t length):
        """Parse a chunk of data into a Python numeric value.

        Note: this is a parser for the on-disk format, not the send/recv
        format. As such it is machine-dependent and probably incomplete.
        """
        cdef Py_ssize_t wpad
        if 0 <= pos <= self._buf.len - length:
            # the format includes the varlena header and alignment padding
            wpad = sizeof(uint32_t) + int32_pad[pos % sizeof(uint32_t)]
            return parse_numeric(
                <unsigned char *>(self._buf.buf + pos + wpad), length - wpad)

        raise IndexError(
            f"can't get {length} bytes from {pos}: buffer size is {self._buf.len}")

    cdef uint32_t _get32(self, Py_ssize_t pos) except? 0xFFFFFFFF:
        """Parse an uint32 from a position in the data buffer.

        Note: parsing little endian here. I assume the bytes order depends on
        the server machine architecture.

        TODO: Sniff it from the root container.
        """
        if 0 <= pos <= self._buf.len - <Py_ssize_t>sizeof(uint32_t):
            return (<uint32_t *>(self._buf.buf + pos))[0]

        raise IndexError(f"can't access {pos}: buffer size is {self._buf.len}")


# The following definitions are converted from Postgres source, and allow
# bit-level access to the JsonEntry and JsonContainer values. See
# https://github.com/postgres/postgres/blob/master/src/include/utils/jsonb.h
# for all the details.

cdef extern from *:
    """
#define JENTRY_OFFLENMASK       0x0FFFFFFF
#define JENTRY_TYPEMASK         0x70000000
#define JENTRY_HAS_OFF          0x80000000

/* values stored in the type bits */
#define JENTRY_ISSTRING         0x00000000
#define JENTRY_ISNUMERIC        0x10000000
#define JENTRY_ISBOOL_FALSE     0x20000000
#define JENTRY_ISBOOL_TRUE      0x30000000
#define JENTRY_ISNULL           0x40000000
#define JENTRY_ISCONTAINER      0x50000000      /* array or object */

/* Access macros.  Note possible multiple evaluations */
#define jbe_offlenfld(je)      ((je) & JENTRY_OFFLENMASK)
#define jbe_has_off(je)        (((je) & JENTRY_HAS_OFF) != 0)
#define jbe_isstring(je)       (((je) & JENTRY_TYPEMASK) == JENTRY_ISSTRING)
#define jbe_isnumeric(je)      (((je) & JENTRY_TYPEMASK) == JENTRY_ISNUMERIC)
#define jbe_iscontainer(je)    (((je) & JENTRY_TYPEMASK) == JENTRY_ISCONTAINER)
#define jbe_isnull(je)         (((je) & JENTRY_TYPEMASK) == JENTRY_ISNULL)
#define jbe_isbool_true(je)    (((je) & JENTRY_TYPEMASK) == JENTRY_ISBOOL_TRUE)
#define jbe_isbool_false(je)   (((je) & JENTRY_TYPEMASK) == JENTRY_ISBOOL_FALSE)
#define jbe_isbool(je)         (JBE_ISBOOL_TRUE(je) || JBE_ISBOOL_FALSE(je))

/* flags for the header-field in JsonbContainer */
#define JB_CMASK        0x0FFFFFFF      /* mask for count field */
#define JB_FSCALAR      0x10000000      /* flag bits */
#define JB_FOBJECT      0x20000000
#define JB_FARRAY       0x40000000

/* convenience macros for accessing a JsonbContainer struct */
#define jc_size(jc)         ((jc) & JB_CMASK)
#define jc_is_scalar(jc)    (((jc) & JB_FSCALAR) != 0)
#define jc_is_object(jc)    (((jc) & JB_FOBJECT) != 0)
#define jc_is_array(jc)     (((jc) & JB_FARRAY) != 0)

/* padding to align pointers to 4-bounds */
static const int int32_pad[] = {0, 3, 2, 1};
    """
    int jbe_offlenfld(JEntry je)
    int jbe_has_off(JEntry je)
    int jbe_isstring(JEntry je)
    int jbe_isnumeric(JEntry je)
    int jbe_iscontainer(JEntry je)
    int jbe_isnull(JEntry je)
    int jbe_isbool_true(JEntry je)
    int jbe_isbool_false(JEntry je)
    int jbe_isbool(JEntry je)

    int jc_size(JCont jc)
    int jc_is_scalar(JCont jc)
    int jc_is_array(JCont jc)
    int jc_is_object(JCont jc)

    const int[4] int32_pad
