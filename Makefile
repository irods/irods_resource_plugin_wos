SUBS = 	wos 

######################################################################
# Configuration should occur above this line

GCC = g++ -DRODS_SERVER 

.PHONY: ${SUBS} clean

default: ${SUBS}

${SUBS}:
	${MAKE} -C $@

clean:
	@-for dir in ${SUBS}; do \
	${MAKE} -C $$dir clean; \
	done
