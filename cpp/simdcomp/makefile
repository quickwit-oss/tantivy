# minimalist makefile
.SUFFIXES:
#
.SUFFIXES: .cpp .o .c .h
ifeq ($(DEBUG),1)
CFLAGS = -fPIC  -std=c89 -ggdb -msse4.1 -march=native -Wall -Wextra -Wshadow -fsanitize=undefined  -fno-omit-frame-pointer -fsanitize=address
else
CFLAGS = -fPIC -std=c89 -O3 -msse4.1  -march=native -Wall -Wextra -Wshadow
endif # debug
LDFLAGS = -shared
LIBNAME=libsimdcomp.so.0.0.3
all:  unit unit_chars bitpackingbenchmark $(LIBNAME)
test:
	./unit
	./unit_chars
install: $(OBJECTS)
	cp $(LIBNAME) /usr/local/lib
	ln -s /usr/local/lib/$(LIBNAME) /usr/local/lib/libsimdcomp.so
	ldconfig
	cp $(HEADERS) /usr/local/include



HEADERS=./include/simdbitpacking.h ./include/simdcomputil.h ./include/simdintegratedbitpacking.h ./include/simdcomp.h ./include/simdfor.h ./include/avxbitpacking.h

uninstall:
	for h in $(HEADERS) ; do rm  /usr/local/$$h; done
	rm  /usr/local/lib/$(LIBNAME)
	rm /usr/local/lib/libsimdcomp.so
	ldconfig


OBJECTS= simdbitpacking.o simdintegratedbitpacking.o simdcomputil.o \
		 simdpackedsearch.o simdpackedselect.o simdfor.o avxbitpacking.o

$(LIBNAME): $(OBJECTS)
	$(CC) $(CFLAGS) -o $(LIBNAME) $(OBJECTS)  $(LDFLAGS)


avxbitpacking.o: ./src/avxbitpacking.c $(HEADERS)
	$(CC) $(CFLAGS) -c ./src/avxbitpacking.c -Iinclude


simdfor.o: ./src/simdfor.c $(HEADERS)
	$(CC) $(CFLAGS) -c ./src/simdfor.c -Iinclude


simdcomputil.o: ./src/simdcomputil.c $(HEADERS)
	$(CC) $(CFLAGS) -c ./src/simdcomputil.c -Iinclude

simdbitpacking.o: ./src/simdbitpacking.c $(HEADERS)
	$(CC) $(CFLAGS) -c ./src/simdbitpacking.c -Iinclude

simdintegratedbitpacking.o: ./src/simdintegratedbitpacking.c  $(HEADERS)
	$(CC) $(CFLAGS) -c ./src/simdintegratedbitpacking.c -Iinclude

simdpackedsearch.o: ./src/simdpackedsearch.c $(HEADERS)
	$(CC) $(CFLAGS) -c ./src/simdpackedsearch.c -Iinclude

simdpackedselect.o: ./src/simdpackedselect.c $(HEADERS)
	$(CC) $(CFLAGS) -c ./src/simdpackedselect.c -Iinclude

example: ./example.c    $(HEADERS) $(OBJECTS)
	$(CC) $(CFLAGS) -o example ./example.c -Iinclude  $(OBJECTS)

unit: ./tests/unit.c    $(HEADERS) $(OBJECTS)
	$(CC) $(CFLAGS) -o unit ./tests/unit.c -Iinclude  $(OBJECTS)

bitpackingbenchmark: ./benchmarks/bitpackingbenchmark.c    $(HEADERS) $(OBJECTS)
	$(CC) $(CFLAGS) -o bitpackingbenchmark ./benchmarks/bitpackingbenchmark.c -Iinclude  $(OBJECTS)
benchmark: ./benchmarks/benchmark.c    $(HEADERS) $(OBJECTS)
	$(CC) $(CFLAGS) -o benchmark ./benchmarks/benchmark.c -Iinclude  $(OBJECTS)
dynunit: ./tests/unit.c    $(HEADERS) $(LIBNAME)
	$(CC) $(CFLAGS) -o dynunit ./tests/unit.c -Iinclude  -lsimdcomp

unit_chars: ./tests/unit_chars.c    $(HEADERS) $(OBJECTS)
	$(CC) $(CFLAGS) -o unit_chars ./tests/unit_chars.c -Iinclude  $(OBJECTS)
clean:
	rm -f unit *.o $(LIBNAME) example benchmark bitpackingbenchmark dynunit unit_chars
