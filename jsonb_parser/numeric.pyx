"""
jsonb_parser -- parser for numeric values.
"""

# Copyright (C) 2021 Daniele Varrazzo

import struct
from typing import cast, Callable, Dict, Tuple, Union
from collections import namedtuple

Buffer = Union[bytes, bytearray, memoryview]

# From 'src/backend/utils/adt/numeric.c'

# Numeric values are represented in a base-NBASE floating point format.
# Each "digit" ranges from 0 to NBASE-1.  The type NumericDigit is signed
# and wide enough to store a digit.  We assume that NBASE*NBASE can fit in
# an int.  [...]
# Values of NBASE other than 10000 are considered of historical interest only
# and are no longer supported in any sense; no mechanism exists for the client
# to discover the base, so every client supporting binary mode expects the
# base-10000 format.

NBASE = 10000

# The Numeric type as stored on disk.

# If the high bits of the first word of a NumericChoice (n_header, or
# n_short.n_header, or n_long.n_sign_dscale) are NUMERIC_SHORT, then the
# numeric follows the NumericShort format; if they are NUMERIC_POS or
# NUMERIC_NEG, it follows the NumericLong format. If they are NUMERIC_SPECIAL,
# the value is a NaN or Infinity.  We currently always store SPECIAL values
# using just two bytes (i.e. only n_header), but previous releases used only
# the NumericLong format, so we might find 4-byte NaNs (though not infinities)
# on disk if a database has been migrated using pg_upgrade.  In either case,
# the low-order bits of a special value's header are reserved and currently
# should always be set to zero.

# NOTE: by convention, values in the packed form have been stripped of
# all leading and trailing zero digits (where a "digit" is of base NBASE).
# In particular, if the value is zero, there will be no digits at all!
# The weight is arbitrary in that case, but we normally set it to zero.

# Interpretation of high bits.
NUMERIC_SIGN_MASK = 0xC000
NUMERIC_POS = 0x0000
NUMERIC_NEG = 0x4000
NUMERIC_SHORT = 0x8000
NUMERIC_SPECIAL = 0xC000


def parse_numeric(data: Buffer) -> Union[float, int]:
    """Parse a chunk of data into a Python numeric value.

    Note: this is a parser for the on-disk format, not the send/recv
    format. As such it is machine-dependent and probably incomplete.
    """
    head = _get16(data, 0)
    hmsb = head & NUMERIC_SIGN_MASK  # head most significant bits
    if hmsb == NUMERIC_SHORT:
        return _parse_short(data)
    elif hmsb == NUMERIC_SPECIAL:
        return _parse_special(head)
    else:
        return _parse_long(data)


# Definitions for special values (NaN, positive infinity, negative infinity).
#
# The two bits after the NUMERIC_SPECIAL bits are 00 for NaN, 01 for positive
# infinity, 11 for negative infinity.  (This makes the sign bit match where
# it is in a short-format value, though we make no use of that at present.)
# We could mask off the remaining bits before testing the active bits, but
# currently those bits must be zeroes, so masking would just add cycles.

NUMERIC_EXT_SIGN_MASK = 0xF000  # high bits plus NaN/Inf flag bits
NUMERIC_NAN = 0xC000
NUMERIC_PINF = 0xD000
NUMERIC_NINF = 0xF000
NUMERIC_INF_SIGN_MASK = 0x2000


def _parse_special(
    head: int,
    __specials: Dict[int, float] = {
        NUMERIC_NAN: float("NaN"),
        NUMERIC_PINF: float("Inf"),
        NUMERIC_NINF: float("-Inf"),
    },
) -> float:
    return __specials[head]


# In the NumericShort format, the remaining 14 bits of the header word
# (n_short.n_header) are allocated as follows: 1 for sign (positive or
# negative), 6 for dynamic scale, and 7 for weight.  In practice, most
# commonly-encountered values can be represented this way.

NUMERIC_SHORT_SIGN_MASK = 0x2000
NUMERIC_SHORT_DSCALE_MASK = 0x1F80
NUMERIC_SHORT_DSCALE_SHIFT = 7
NUMERIC_SHORT_DSCALE_MAX = (
    NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT
)
NUMERIC_SHORT_WEIGHT_SIGN_MASK = 0x0040
NUMERIC_SHORT_WEIGHT_MASK = 0x003F
NUMERIC_SHORT_WEIGHT_MAX = NUMERIC_SHORT_WEIGHT_MASK
NUMERIC_SHORT_WEIGHT_MIN = -(NUMERIC_SHORT_WEIGHT_MASK + 1)


def _parse_short(data: Buffer) -> Union[int, float]:
    head = _get16(data, 0)

    # assemble the integer mantissa
    num: Union[int, float] = 0

    # weight, in NBASE-digits
    weight = head & NUMERIC_SHORT_WEIGHT_MASK
    if head & NUMERIC_SHORT_WEIGHT_SIGN_MASK:
        weight |= ~NUMERIC_SHORT_WEIGHT_MASK

    for p in range(2, len(data), 2):
        num = num * 10_000 + _get16(data, p)

    ndigits = len(data) // 2 - 1
    shift = ndigits - weight - 1
    if shift > 0:
        for _ in range(shift):
            num /= 10_000
    elif shift < 0:
        for _ in range(-shift):
            num *= 10_000

    if head & NUMERIC_SHORT_SIGN_MASK:
        num = -num
    return num


ShortDetails = namedtuple("ShortDetails", "dscale weight sign digits")


def dis_short(data: bytes) -> ShortDetails:
    """Debug helper to see what's in a short numeric"""
    head = _get16(data, 0)
    assert (head & NUMERIC_SIGN_MASK) == NUMERIC_SHORT, "not a short"

    dscale = (head & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT
    weight = head & NUMERIC_SHORT_WEIGHT_MASK
    if head & NUMERIC_SHORT_WEIGHT_SIGN_MASK:
        weight |= ~NUMERIC_SHORT_WEIGHT_MASK

    digits = tuple(_get16(data, i) for i in range(2, len(data), 2))
    sign = "-" if head & NUMERIC_SHORT_SIGN_MASK else "+"

    return ShortDetails(dscale=dscale, weight=weight, sign=sign, digits=digits)


# In the NumericLong format, the remaining 14 bits of the header word
# (n_long.n_sign_dscale) represent the display scale; and the weight is
# stored separately in n_weight.


def _parse_long(data: Buffer) -> Union[float, int]:
    raise NotImplementedError("long numeric not implemented yet")


def _get16(data: Buffer, pos: int) -> int:
    """Parse an uint16 from a position in the data buffer.

    Note: parsing little endian here. I assume the bytes order depends on
    the server machine architecture.
    """
    return _unpack_uint2(data, pos)[0]


_UnpackInt = Callable[[Buffer, int], Tuple[int]]

# TODO: the server might be big-endian. Detect from first bytes?
_unpack_uint2 = cast(_UnpackInt, struct.Struct("<H").unpack_from)
