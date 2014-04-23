PREFIX=/usr/local
DESTDIR=
GOFLAGS=
BINDIR=${PREFIX}/bin

API_SRCS = $(wildcard apps/api/*.go )
BROKER_SRCS = $(wildcard apps/broker/*.go )
WORKER_SRCS = $(wildcard apps/worker/*.go )
CLIENT_SRCS = $(wildcard apps/client/*.go )
BENCH_CLIENT_SRC= $(wildcard bench/bench_client/*.go )
# NSQADMIN_SRCS = $(wildcard nsqadmin/*.go nsqadmin/templates/*.go util/*.go)

# BINARIES = nsqadmin
APPS = api broker worker client 
BENCHS = bench_client
BLDDIR = build

all: $(APPS) $(BENCHS)

$(BLDDIR)/%:
	@mkdir -p $(dir $@)
	go build ${GOFLAGS} -o $(abspath $@) ./$*

$(BINARIES): %: $(BLDDIR)/%
$(APPS): %: $(BLDDIR)/apps/%
$(BENCHS): %: $(BLDDIR)/bench/%

$(BLDDIR)/apps/api: $(API_SRCS)
$(BLDDIR)/apps/broker: $(BROKER_SRCS)
$(BLDDIR)/apps/worker: $(WORKER_SRCS)
$(BLDDIR)/apps/client: $(CLIENT_SRCS)
$(BLDDIR)/bench/bench_client: $(BENCH_CLIENT_SRCS)

clean:
	rm -fr $(BLDDIR)

.PHONY: install clean all
.PHONY: $(BINARIES)
.PHONY: $(APPS)

install: $(EXAMPLES)
	install -m 755 -d ${DESTDIR}${BINDIR}
	install -m 755 $(BLDDIR)/apps/api ${DESTDIR}${BINDIR}/api
	install -m 755 $(BLDDIR)/apps/broker ${DESTDIR}${BINDIR}/broker
	install -m 755 $(BLDDIR)/apps/worker ${DESTDIR}${BINDIR}/worker
	install -m 755 $(BLDDIR)/apps/client ${DESTDIR}${BINDIR}/client
	install -m 755 $(BLDDIR)/bench/bench_client ${DESTDIR}${BINDIR}/bench_client

