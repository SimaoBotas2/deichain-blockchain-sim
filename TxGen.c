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

#define DEBUG // Remove esta linha para remover as mensagens de debug
#define SHM_KEY 0x1234 // Chave para segmento de memória compartilhado

#define MAX_TRANSACTIONS 100 // Temporário

int main(int argc, char *argv[]) {
    if (argc != 3) {
        printf("Uso correto: %s <reward> <sleep time>\n", argv[0]);
        return -1;
    }

    int reward = atoi(argv[1]);
    int sleeptime = atoi(argv[2]);

    int shmid = shmget(SHM_KEY, sizeof(SharedMemory), 0666);
    if (shmid < 0) {
        perror("Erro shmget (TxGen)");
        exit(1);
    }

    // Anexar a memória compartilhada
    SharedMemory *shrd = (SharedMemory *)shmat(shmid, NULL, 0);
    if (shrd == (void *)(-1)) {
        perror("Erro shmat (TxGen)");
        exit(1);
    }

    // NÃO inicializar o mutex aqui! Ele já deve estar inicializado no controller.

    // Geração de transações
    int transaction_id = 1;

    while (1) {
        sem_wait(&(shrd->sem));

        if (shrd->transaction_count < MAX_TRANSACTIONS) {
            Transaction new_tx;
            new_tx.id = transaction_id++;
            snprintf(new_tx.details, sizeof(new_tx.details), "Transaction %d - Reward: %d", new_tx.id, reward);

            // Guardar na memória compartilhada
            shrd->transactions[shrd->transaction_count] = new_tx;
            shrd->transaction_count++;

            printf("Transação gerada %d: %s\n", new_tx.id, new_tx.details);
        } 
        else {
            printf("Transaction buffer cheio. À espera...\n");
        }
        
        sem_post(&(shrd->sem));

        sleep(sleeptime);
    }

    shmdt(shrd);

    return 0;
}