all: txgen SO

txgen: txgen.c
	gcc -pthread txgen.c -o txgen

SO: SO.c pow.c pow.h
	gcc -pthread SO.c pow.c -o SO