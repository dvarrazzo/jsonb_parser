-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION ubjson" to load this file. \quit

-- shell type
CREATE TYPE ubjson;


CREATE OR REPLACE FUNCTION ubjson_in(cstring)
 RETURNS ubjson
 LANGUAGE internal
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$jsonb_in$function$;

CREATE OR REPLACE FUNCTION ubjson_out(ubjson)
 RETURNS cstring
 LANGUAGE internal
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$jsonb_out$function$;


-- TODO: these are the functions to replace
CREATE OR REPLACE FUNCTION ubjson_send(ubjson)
 RETURNS bytea
 LANGUAGE internal
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$jsonb_send$function$;

CREATE OR REPLACE FUNCTION ubjson_recv(internal)
 RETURNS ubjson
 LANGUAGE internal
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$jsonb_recv$function$;


CREATE TYPE ubjson (
    INPUT = ubjson_in,
    OUTPUT = ubjson_out,
    RECEIVE = ubjson_recv,
    SEND = ubjson_send,
    LIKE = jsonb
);


-- Allow cast without copy
CREATE CAST (jsonb AS ubjson) WITHOUT FUNCTION AS ASSIGNMENT;
CREATE CAST (ubjson AS jsonb) WITHOUT FUNCTION AS ASSIGNMENT;
