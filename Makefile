all: txgen SO

txgen: txgen.c
	gcc -pthread TxGen.c -o TxGen

SO: SO.c
	gcc -pthread SO.c -o SO

