<<<<<<< HEAD
=======
/*
    DEIChain: A Concurrency-Focused Blockchain Simulation
    Copyright (c) 2025
    Authors: Francisco Teixeira (2023223276)
             Simão Botas (2021223055)
 
*/

>>>>>>> 6546a6966b5c518be8791208f715995792999644
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <semaphore.h>
<<<<<<< HEAD
=======

>>>>>>> 6546a6966b5c518be8791208f715995792999644
#include "structs.h"

#define DEBUG // Remove esta linha para remover as mensagens de debug
#define SHM_KEY 0x1234 // Chave para segmento de memória compartilhado

<<<<<<< HEAD

void sleep_ms(int sleeptime){
    struct timespec ts;
    ts.tv_sec = sleeptime / 1000;
    ts.tv_nsec = (sleeptime %1000) * 1000000;
    nanosleep(&ts,NULL);
}


=======
#define MAX_TRANSACTIONS 100 // Temporário
>>>>>>> 6546a6966b5c518be8791208f715995792999644

int main(int argc, char *argv[]) {
    if (argc != 3) {
        printf("Uso correto: %s <reward> <sleep time>\n", argv[0]);
        return -1;
    }

<<<<<<< HEAD
    //Verificação de Inputs


    int reward = atoi(argv[1]);

    if(reward <1 || reward >3){
        printf("Reward deve ser entre 1 e 3\n");
        return -1;
    }

    int sleeptime = atoi(argv[2]);

    if(sleeptime <200 || sleeptime >3000){
        printf("Sleeptime deve ser entre 200 e 3000 (ms)\n");
        return -1;
    }

    int shmid = shmget(SHM_KEY, sizeof(TransactionPool), 0666);
=======
    int reward = atoi(argv[1]);
    int sleeptime = atoi(argv[2]);

    int shmid = shmget(SHM_KEY, sizeof(SharedMemory), 0666);
>>>>>>> 6546a6966b5c518be8791208f715995792999644
    if (shmid < 0) {
        perror("Erro shmget (TxGen)");
        exit(1);
    }
<<<<<<< HEAD
    
    //Anexar a memória compartilhada
    TransactionPool *shrd = (TransactionPool * )shmat(shmid,NULL,0);
    if (shrd == (void *)(-1)) {
        perror("shmat error");
        exit(1);
    }

    //Geração de transações
    int transaction_id = 1;  
=======

    // Anexar a memória compartilhada
    SharedMemory *shrd = (SharedMemory *)shmat(shmid, NULL, 0);
    if (shrd == (void *)(-1)) {
        perror("Erro shmat (TxGen)");
        exit(1);
    }

    // NÃO inicializar o mutex aqui! Ele já deve estar inicializado no controller.

    // Geração de transações
    int transaction_id = 1;
>>>>>>> 6546a6966b5c518be8791208f715995792999644

    while (1) {
        sem_wait(&(shrd->sem));

<<<<<<< HEAD
        for(int i =0;i<shrd->pool_size;i++){

        //Procura uma entry vazia
        if (shrd->entries[i].empty ) {
            Transaction new_tx;
            new_tx.id = getpid(); //Apenas pra ter um valor
            new_tx.reward = reward;
            new_tx.value = (rand() % 100) + 1; // Valor random (diz no enunciado)
            snprintf(new_tx.details, sizeof(new_tx.details), "Transaction %d - Reward: %d", new_tx.id, reward);

            //Guardar na memoria partilhada
            shrd->entries[i].tx= new_tx;
            shrd->entries[i].empty = false;
            shrd->entries[i].age = 0;
            shrd->transaction_pending_set++;
=======
        if (shrd->transaction_count < MAX_TRANSACTIONS) {
            Transaction new_tx;
            new_tx.id = transaction_id++;
            snprintf(new_tx.details, sizeof(new_tx.details), "Transaction %d - Reward: %d", new_tx.id, reward);

            // Guardar na memória compartilhada
            shrd->transactions[shrd->transaction_count] = new_tx;
            shrd->transaction_count++;
>>>>>>> 6546a6966b5c518be8791208f715995792999644

            printf("Transação gerada %d: %s\n", new_tx.id, new_tx.details);
        } 
        else {
            printf("Transaction buffer cheio. À espera...\n");
        }
<<<<<<< HEAD
        sem_post(&(shrd->sem));

        sleep_ms(sleeptime);
    }
}

    shmdt(shrd);
    return 0;
	}


=======
        
        sem_post(&(shrd->sem));

        sleep(sleeptime);
    }

    shmdt(shrd);

    return 0;
}
>>>>>>> 6546a6966b5c518be8791208f715995792999644
