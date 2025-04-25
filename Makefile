all: txgen SO

txgen: txgen.c
<<<<<<< HEAD
	gcc -pthread txgen.c -o txgen
=======
	gcc -pthread TxGen.c -o TxGen
>>>>>>> 6546a6966b5c518be8791208f715995792999644

SO: SO.c
	gcc -pthread SO.c -o SO

