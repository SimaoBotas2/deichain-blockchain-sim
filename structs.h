/*
    DEIChain: A Concurrency-Focused Blockchain Simulation
    Copyright (c) 2025
    Authors: Francisco Teixeira (2023223276)
             Simão Botas (2021223055)
 
*/

#include <semaphore.h>

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
    int sender_id;
    int receiver_id;
    int value;
    char details[50];
} Transaction;

typedef struct SharedMemory{
    int transaction_count; // Número atual de transações na pool
    Transaction transactions[100]; //Temporário  , array dinamico, stack, etc...
    sem_t sem;
} SharedMemory;

#endif