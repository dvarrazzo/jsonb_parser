#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/jsonb.h"
#include "ubjson_numeric.h"

PG_MODULE_MAGIC;


static void jsonb_send_ubjson(StringInfo out, JsonbContainer *in, int estimated_len);
static void ubjson_put_value(StringInfo out, JsonbValue *scalarVal);
static void ubjson_put_string(StringInfo out, JsonbValue *scalarVal);
static void ubjson_put_numeric(StringInfo out, JsonbValue *scalarVal);


PG_FUNCTION_INFO_V1(ubjson_recv);
Datum
ubjson_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int			version = pq_getmsgint(buf, 1);

	/* TODO: on 1, parse jsonb */
	if (version == 2)
	{
		/* TODO */
	}
	else
		elog(ERROR, "unsupported ubjson version number %d", version);

	elog(ERROR, "not implemented");
}


PG_FUNCTION_INFO_V1(ubjson_send);
Datum
ubjson_send(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	StringInfoData buf;
	/* Todo: can do without this, writing directly in buf? */
	StringInfo	jtext = makeStringInfo();
	int			version = 2;

	/* TODO: varsize is probably a gross overestimation? */
	(void) jsonb_send_ubjson(jtext, &jb->root, VARSIZE(jb));

	pq_begintypsend(&buf);
	pq_sendint8(&buf, version);
	pq_sendtext(&buf, jtext->data, jtext->len);
	pfree(jtext->data);
	pfree(jtext);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

void
jsonb_send_ubjson(StringInfo out, JsonbContainer *in, int estimated_len)
{
	JsonbIterator *it;
	JsonbValue	v;
	JsonbIteratorToken type = WJB_DONE;
	bool		redo_switch = false;
	bool		raw_scalar = false;

	enlargeStringInfo(out, (estimated_len >= 0) ? estimated_len : 64);

	it = JsonbIteratorInit(in);

	while (redo_switch ||
		   ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE))
	{
		redo_switch = false;
		switch (type)
		{
			case WJB_BEGIN_ARRAY:
				if (!v.val.array.rawScalar)
					appendStringInfoCharMacro(out, '[');
				else
					raw_scalar = true;
				break;
			case WJB_BEGIN_OBJECT:
				appendStringInfoCharMacro(out, '{');
				break;
			case WJB_KEY:

				/* json rules guarantee this is a string */
				ubjson_put_string(out, &v);

				type = JsonbIteratorNext(&it, &v, false);
				if (type == WJB_VALUE)
				{
					ubjson_put_value(out, &v);
				}
				else
				{
					Assert(type == WJB_BEGIN_OBJECT || type == WJB_BEGIN_ARRAY);

					/*
					 * We need to rerun the current switch() since we need to
					 * output the object which we just got from the iterator
					 * before calling the iterator again.
					 */
					redo_switch = true;
				}
				break;
			case WJB_ELEM:
				ubjson_put_value(out, &v);
				break;
			case WJB_END_ARRAY:
				if (!raw_scalar)
					appendStringInfoCharMacro(out, ']');
				break;
			case WJB_END_OBJECT:
				appendStringInfoCharMacro(out, '}');
				break;
			default:
				elog(ERROR, "unknown jsonb iterator token type");
		}
	}
}


void
ubjson_put_value(StringInfo out, JsonbValue *scalarVal)
{
	switch (scalarVal->type)
	{
		case jbvNull:
			appendStringInfoCharMacro(out, 'Z');
			break;

		case jbvString:
			appendStringInfoCharMacro(out, 'S');
			ubjson_put_string(out, scalarVal);
			break;

		case jbvNumeric:
			ubjson_put_numeric(out, scalarVal);
			break;

		case jbvBool:
			if (scalarVal->val.boolean)
				appendStringInfoCharMacro(out, 'T');
			else
				appendStringInfoCharMacro(out, 'F');
			break;

		default:
			elog(ERROR, "unknown jsonb scalar type");
	}
}

void
ubjson_put_string(StringInfo out, JsonbValue *scalarVal)
{
	/* Note:
	 * don't add the 'S' token, so the function can be used for object keys.
	 */
	if (scalarVal->val.string.len < 256) {
		appendStringInfoCharMacro(out, 'U');
		appendStringInfoCharMacro(out, (uint8)scalarVal->val.string.len);
	}
	else if (scalarVal->val.string.len <  32768) {
		appendStringInfoCharMacro(out, 'I');
		pq_writeint16(out, (uint16)scalarVal->val.string.len);
	}
	else {
		appendStringInfoCharMacro(out, 'l');
		pq_writeint32(out, (uint32)scalarVal->val.string.len);
	}
	appendBinaryStringInfo(out, scalarVal->val.string.val, scalarVal->val.string.len);
}


void
ubjson_put_numeric(StringInfo out, JsonbValue *scalarVal)
{
	Numeric num = DatumGetNumeric(PointerGetDatum(scalarVal->val.numeric));

	/* This function should be implemented in utils/adt/numeric.c as it
	 * uses the private Decimal data */
	numeric_append_ubjson(out, num);
}
