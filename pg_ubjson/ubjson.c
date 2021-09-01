#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"

PG_MODULE_MAGIC;


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
    int			version = 2;
    /* TODO */
    char *str = "T";

	StringInfoData buf;
	pq_begintypsend(&buf);
	pq_sendint8(&buf, version);
	pq_sendtext(&buf, str, strlen(str));

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
