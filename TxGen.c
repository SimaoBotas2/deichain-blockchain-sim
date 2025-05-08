/*
    DEIChain: A Concurrency-Focused Blockchain Simulation
    Copyright (c) 2025
    Authors: Francisco Teixeira (2023223276)
             Simão Botas (2021223055)
 
*/

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
#include "structs.h"
#include <stdbool.h>

#define DEBUG // Remove esta linha para remover as mensagens de debug
#define SHM_KEY 0x1234 // Chave para segmento de memória compartilhado


void sleep_ms(int sleeptime){
    struct timespec ts;
    ts.tv_sec = sleeptime / 1000;
    ts.tv_nsec = (sleeptime %1000) * 1000000;
    nanosleep(&ts,NULL);
}



int main(int argc, char *argv[]) {
    if (argc != 3) {
        printf("Uso correto: %s <reward> <sleep time>\n", argv[0]);
        return -1;
    }

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
    if (shmid < 0) {
        perror("Erro shmget (TxGen)");
        exit(1);
    }
    
    //Anexar a memória compartilhada
    TransactionPool *shrd = (TransactionPool * )shmat(shmid,NULL,0);
    if (shrd == (void *)(-1)) {
        perror("shmat error");
        exit(1);
    }

    //Geração de transações
    int transaction_id = 1;  

    while (1) {
        sem_wait(&(shrd->sem));

        for(int i =0;i<shrd->pool_size;i++){

        //Procura uma entry vazia
        if (shrd->entries[i].empty ) {
            Transaction new_tx;
            snprintf(new_tx.id, sizeof(new_tx.id), "TX%d-%d", getpid(), transaction_id);
            new_tx.reward = reward;
            new_tx.value = (rand() % 100) + 1; // Valor random (diz no enunciado)

            //Guardar na memoria partilhada
            shrd->entries[i].tx= new_tx;
            shrd->entries[i].empty = false;
            shrd->entries[i].age = 0;
            shrd->transaction_pending_set++;

            printf("Transação %s gerada , com valor: %d\n", new_tx.id, new_tx.value);
        } 
        else {
            printf("Transaction buffer cheio. À espera...\n");
        }

        transaction_id++;
        sem_post(&(shrd->sem));

        
        sleep_ms(sleeptime);
    }
}

    shmdt(shrd);
    return 0;
	}


