CFLAGS += -Wall -Wextra -Werror -Wno-unused-parameter
CFLAGS += -I $(shell pg_config --includedir)

pg_ddos: pg_ddos.o
	$(CC) $(CFLAGS) $+ -luv -lpq -o $@

%.o: %.c %.h Makefile
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o pg_ddos
