/*
    DEIChain: A Concurrency-Focused Blockchain Simulation
    Copyright (c) 2025
    Authors: Francisco Teixeira (2023223276)
             Simão Botas (2021223055)
 
*/

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include <time.h>
#include <semaphore.h>  

#ifndef STRUCTS_H
#define STRUCTS_H

#define TX_ID_LEN 64
#define TXB_ID_LEN 64
#define HASH_SIZE 65  // SHA256_DIGEST_LENGTH * 2 + 1
#define STATS_MTYPE 1

extern int transactions_per_block;

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
    char id[TX_ID_LEN];
    int reward;
    int value;
    time_t timestamp; 
} Transaction;

//Estrutura para guardar dados sobre as transações
typedef struct TransactionEntry{
    bool empty; //indica se a entry está disponível ou não
    int age; //contador para idade da entry (usada no validator)
    Transaction tx;
}TransactionEntry;

typedef struct StatMessage {
    long mtype;             // necessário para message queue
    int miner_id;           // ID do miner
    bool valid;              // 1 se válido, 0 se inválido
    int credits;            // soma das recompensas (apenas se válido)
    time_t tx_start_time;   // timestamp da transação mais antiga
    time_t block_time;      // timestamp do bloco
} StatMessage;

typedef struct TransactionPool{
    int transaction_pending_set; // Número atual de transações na pool
    int pool_size;  //Tamanho da transaction pool, definido pelo ficheiro de configuração
    sem_t sem;
    TransactionEntry entries[];
}TransactionPool;

// Estrutura para um bloco da blockchain
typedef struct Block {
    char id[TXB_ID_LEN]; 
    char previous_hash[HASH_SIZE]; 
    char hash[HASH_SIZE];
    time_t timestamp; 
    unsigned int nonce; // Nº encontrado pelo PoW(proof of work)
    int transactions_count;
    int miner_id;
    Transaction transactions[]; 
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

// Inline function to compute the size of a TransactionBlock
static inline size_t get_transaction_block_size() {
    if (transactions_per_block == 0) {
      perror("Must set the 'transactions_per_block' variable before using!\n");
      exit(-1);
    }
    return sizeof(Block) + transactions_per_block * sizeof(Transaction);
  }


#endif