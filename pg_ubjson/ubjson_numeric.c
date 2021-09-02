/* Extend this file */
#include "numeric.c.h"

#include "ubjson_numeric.h"


void numeric_append_ubjson(StringInfo out, Numeric num)
{
	NumericVar var;

	if (NUMERIC_IS_SHORT(num))
	{
		/* Can't use a compact representation if the numer has decimal digits */
		if (NUMERIC_DSCALE(num) == 0) {
			int64 v64;

			init_var_from_num(num, &var);
			if (numericvar_to_int64(&var, &v64)) {
				/* It fits in 64 bits. Can we ask for less? */
				if (PG_INT16_MIN <= v64 && v64 <= PG_INT16_MAX) {
					if (PG_INT8_MIN <= v64 && v64 <= PG_INT8_MAX) {
						appendStringInfoCharMacro(out, 'i');
						appendStringInfoCharMacro(out, (int8)v64);
					}
					else if (0 <= v64 && v64 <= PG_UINT8_MAX) {
						appendStringInfoCharMacro(out, 'U');
						appendStringInfoCharMacro(out, (uint8)v64);
					}
					else {
						appendStringInfoCharMacro(out, 'I');
						pq_sendint16(out, (int16)v64);
					}
				} else {
					if (PG_INT32_MIN <= v64 && v64 <= PG_INT32_MAX) {
						appendStringInfoCharMacro(out, 'l');
						pq_sendint32(out, (int32)v64);
					}
					else {
						appendStringInfoCharMacro(out, 'L');
						pq_sendint64(out, v64);
					}
				}
				return;
			}
		}
	}

	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_NAN(num))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot convert NaN to json")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot convert infinity to json")));
	}

	/* long format */
	{
		char *str;
		size_t len;

		init_var_from_num(num, &var);
		str = get_str_from_var(&var);
		len = strlen(str);

		appendStringInfoCharMacro(out, 'H');
		if (len <= PG_UINT8_MAX) {
			appendStringInfoCharMacro(out, 'U');
			appendStringInfoCharMacro(out, (uint8)len);
		}
		else if (len <= PG_INT16_MAX) {
			appendStringInfoCharMacro(out, 'I');
			pq_sendint16(out, (int16)len);
		}
		else {
			appendStringInfoCharMacro(out, 'l');
			pq_sendint32(out, (int32)len);
		}
		appendBinaryStringInfo(out, str, len);

		pfree(str);
	}
}
