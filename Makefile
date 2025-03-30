all: txgen SO

txgen: txgen.c
	gcc -pthread txgen.c -o txgen

SO: SO.c
	gcc -pthread SO.c -o SO

