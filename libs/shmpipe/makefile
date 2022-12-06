libshmpipe.a: shmpipe.c
	gcc -c -pthread -g -O2 shmpipe.c
	ar  -cru libshmpipe.a shmpipe.o

libshmpipe.so: shmpipe.c
	gcc -pthread -o libshmpipe.so -shared -g -O2 shmpipe.c

shmpipe: shmpipe.c
	gcc -pthread -o shmpipe -DSHMEM_PIPE_TEST -g -O2 shmpipe.c -lrt

install:
	sudo cp libshmpipe.so /usr/lib
	sudo cp shmpipe.h /usr/include
