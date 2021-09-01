EXTENSION = ubjson
DATA = ubjson--0.0.1.sql
MODULE_big = ubjson

SRC_C = $(sort $(wildcard *.c))
SRC_H = $(sort $(wildcard *.h))
SRCFILES = $(SRC_C) $(SRC_H)
OBJS = $(patsubst %.c,%.o,$(SRC_C))

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
