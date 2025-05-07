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
    int TRANSACTIONS_PER_BLOCK;
    int BLOCKCHAIN_BLOCKS;
    int TX_POOL_SIZE;
    sem_t sem;
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

// Estrutura para um bloco da blockchain
typedef struct Block {
    int block_id; // Talvez seja char
    int previous_hash;  // Talvez seja char
    int num_transactions;
    unsigned long timestamp; 
    Transaction *transactions; 
    int nonce; // Nº encontrado pelo PoW(proof of work)
    int miner_id;
} Block;

// Estrutura para a blockchain
typedef struct Blockchain {
    int max_blocks;     // BLOCKCHAIN_BLOCKS
    int current_blocks; // Quantos blocos já foram minerados
    Block *blocks;    
    sem_t sem;          // Semáforo para sincronizar acesso à Blockchain
} Blockchain;

typedef struct MinerStats {
    int miner_id;
    int blocks_mined;
    /*
    int valid_blocks;
    int unvalid_blocks;
    */
    int total_reward;
} MinerStats;


#endif