libshmpipe.so: shmpipe.c
	gcc -pthread -o libshmpipe.so -shared -g -O2 shmpipe.c

install:
	sudo cp libshmpipe.so /usr/lib
	sudo cp shmpipe.h /usr/include
