/*
    DEIChain: A Concurrency-Focused Blockchain Simulation
    Copyright (c) 2025
    Authors: Francisco Teixeira (2023223276)
             Simão Botas (2021223055)
 
*/

#include <semaphore.h>
#include <stdbool.h>

#ifndef STRUCTS_H
#define STRUCTS_H

// Estrutura de configuração
typedef struct Configuration {
    int NUM_MINER;
    int POOL_SIZE;
    int TRANSACTIONS_PER_BLOCK;
    int BLOCKCHAIN_BLOCKS;
    int TRANSACTION_POOL_SIZE;
} Config;

// Estrutura para transações
typedef struct Transaction {
    int id;
    int reward;
    int value;
    char details[50];
} Transaction;

//Estrutura para guardar dados sobre as transações
typedef struct TransactionEntry{
    bool empty; //indica se a entry está disponível ou não
    int age; //contador para idade da entry (usada no validator)
    Transaction tx;
}TransactionEntry;


typedef struct TransactionPool{
    int transaction_pending_set; // Número atual de transações na pool
    int pool_size;  //Tamanho da transaction pool, definido pelo ficheiro de configuração
    TransactionEntry * entries;
    sem_t sem;
}TransactionPool;





#endif