#CFLAGS+=-Wall -Wextra -Werror -Wno-unused-parameter -Wno-unused-function
#CFLAGS+=-Wall -Wextra -Werror -Wpedantic
CFLAGS+=-Wall -Wextra -Werror -Wno-unused-parameter
ifeq (true,$(DEBUG))
    CFLAGS+=-O0 -g
else
    CFLAGS+=-O3 -s
endif

pg_ddos: main.o
	$(CC) $(CFLAGS) $+ -luv -lpq -o $@

%.o: %.c %.h Makefile
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o pg_ddos
