#include "postgres.h"
#include "fmgr.h"

#include "ubjson_numeric.h"

void numeric_append_ubjson(StringInfo out, Numeric num)
{
	/* TODO: for real */
	appendStringInfoCharMacro(out, 'i');
	appendStringInfoCharMacro(out, 42);
}
