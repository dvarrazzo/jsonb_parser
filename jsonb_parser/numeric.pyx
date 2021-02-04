"""
jsonb_parser -- parser for numeric values.

See the Python implementation in numeric.py for a description of the format.
"""

# Copyright (C) 2021 Daniele Varrazzo

from libc.stdint cimport uint16_t


cdef object parse_numeric(unsigned char *data, Py_ssize_t length):
    """Parse a chunk of data into a Python numeric value.

    Note: this is a parser for the on-disk format, not the send/recv
    format. As such it is machine-dependent and probably incomplete.
    """
    cdef uint16_t head = _get16(data, length, 0)
    if numeric_is_short(head):
        return _parse_short(data, length)
    elif numeric_is_special(head):
        return _parse_special(head)
    else:
        return _parse_long(data, length)

_nan = float("NaN")
_pinf = float("Inf")
_ninf = float("-Inf")

cdef object _parse_special(head: uint16_t):
    if numeric_is_nan(head):
        return _nan
    elif numeric_is_pinf(head):
        return _pinf
    elif numeric_is_ninf(head):
        return _ninf
    else:
        raise ValueError(f"bad special numeric header: 0x{head:04x}")


cdef object _parse_short(unsigned char *data, Py_ssize_t length):
    cdef uint16_t head = _get16(data, length, 0)

    # assemble the integer mantissa
    num: Union[int, float] = 0

    # weight, in NBASE-digits
    cdef int weight = numeric_short_weight(head)
    cdef int i
    for i in range(2, length, 2):
        num = num * 10000 + _get16(data, length, i)

    cdef int ndigits = (length >> 1) - 1
    cdef int shift = ndigits - weight - 1
    if shift > 0:
        for i in range(shift):
            num *= 0.0001
    elif shift < 0:
        for i in range(-shift):
            num *= 10000

    if head & NUMERIC_SHORT_SIGN_MASK:
        return -num
    else:
        return num


cdef object _parse_long(unsigned char *data, Py_ssize_t length):
    raise NotImplementedError("long numeric not implemented yet")


cdef uint16_t _get16(
    unsigned char *data, Py_ssize_t length, Py_ssize_t pos
) except? 0xFFFF:
    """Parse an uint16 from a position in the data buffer.

    Note: parsing little endian here. I assume the bytes order depends on
    the server machine architecture.
    """
    if 0 <= pos <= length - <Py_ssize_t>sizeof(uint16_t):
        return (<uint16_t *>(data + pos))[0]

    raise IndexError(f"can't access {pos}: buffer size is {length}")


cdef extern from *:
    """
/*
 * Interpretation of high bits.
 */

#define NUMERIC_SIGN_MASK       0xC000
#define NUMERIC_POS             0x0000
#define NUMERIC_NEG             0x4000
#define NUMERIC_SHORT           0x8000
#define NUMERIC_SPECIAL         0xC000

#define NUMERIC_FLAGBITS(h)     ((h) & NUMERIC_SIGN_MASK)
#define numeric_is_short(h)     (NUMERIC_FLAGBITS(h) == NUMERIC_SHORT)
#define numeric_is_special(h)   (NUMERIC_FLAGBITS(h) == NUMERIC_SPECIAL)

#define NUMERIC_EXT_SIGN_MASK   0xF000  /* high bits plus NaN/Inf flag bits */
#define NUMERIC_NAN             0xC000
#define NUMERIC_PINF            0xD000
#define NUMERIC_NINF            0xF000
#define NUMERIC_INF_SIGN_MASK   0x2000

#define NUMERIC_EXT_FLAGBITS(h) ((h) & NUMERIC_EXT_SIGN_MASK)
#define numeric_is_nan(h)       ((h) == NUMERIC_NAN)
#define numeric_is_pinf(h)      ((h) == NUMERIC_PINF)
#define numeric_is_ninf(h)      ((h) == NUMERIC_NINF)
#define numeric_is_inf(h)       (((h) & ~NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF)

/*
 * Short format definitions.
 */

#define NUMERIC_SHORT_SIGN_MASK         0x2000
#define NUMERIC_SHORT_DSCALE_MASK       0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT      7
#define NUMERIC_SHORT_DSCALE_MAX        \
        (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK  0x0040
#define NUMERIC_SHORT_WEIGHT_MASK       0x003F
#define NUMERIC_SHORT_WEIGHT_MAX        NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN        (-(NUMERIC_SHORT_WEIGHT_MASK+1))

#define NUMERIC_DSCALE_MASK             0x3FFF

#define numeric_short_dscale(h) \
        (((h) & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT)
#define numeric_short_weight(h) \
    (((h) & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? ~NUMERIC_SHORT_WEIGHT_MASK : 0) \
     | ((h) & NUMERIC_SHORT_WEIGHT_MASK))
    """
    int numeric_is_short(uint16_t h)
    int numeric_is_special(uint16_t h)
    int numeric_is_nan(uint16_t h)
    int numeric_is_pinf(uint16_t h)
    int numeric_is_ninf(uint16_t h)
    int numeric_short_dscale(uint16_t h)
    int numeric_short_weight(uint16_t h)
    uint16_t NUMERIC_SHORT_SIGN_MASK
