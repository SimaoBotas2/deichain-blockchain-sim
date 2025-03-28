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

#define DEBUG // Remove esta linha para remover as mensagens de debug
#define SHM_KEY 0x1234 // Chave para segmento de memória compartilhado

// Estrutura de configuração
typedef struct Configuration {
    int NUM_MINER;
    int TX_POOL_SIZE;
    int TRANSACTIONS_PER_BLOCK;
    int BLOCKCHAIN_BLOCKS;
} Config;

// Estrutura para transações
typedef struct Transaction {
    int id;
    char details[50];
} Transaction;

typedef struct SharedMemory{

    Transaction transactions[]; //Não sei que tamanho dou a este array
    
    
} SharedMemory;

void controller();
void read_config(const char *filename, Config *config);
void create_ipcs();
void * miner();


// Variáveis globais
int shmid,shrd;

int main() {
    controller();
    return 0;
}


//Processo Miner

void * miner(Config config){

    pthread_t threads[config.NUM_MINER];




}





// Função Controller
void controller(Config config) {

    // Iniciar estrutura
    Config config;

    read_config("config.cfg", &config);

    #ifdef DEBUG
    printf("Configurações carregadas:\n");
    printf("NUM_MINERS: %d\n", config.NUM_MINER);
    printf("TX_POOL_SIZE: %d\n", config.TX_POOL_SIZE);
    printf("TRANSACTIONS_PER_BLOCK: %d\n", config.TRANSACTIONS_PER_BLOCK);
    printf("BLOCKCHAIN_BLOCKS: %d\n", config.BLOCKCHAIN_BLOCKS);
    #endif

    create_ipcs();
    miner(config);
}

// Função para ler o arquivo de configuração
void read_config(const char *filename, Config *config) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Erro ao abrir arquivo de configuração");
        exit(1);
    }

    char key[50];
    int value;
    // Verifica se tem o nome do atributo o seu devido valor
    while (fscanf(file, "%s - %d", key, &value) == 2) {
        if (strcmp(key, "NUM_MINERS") == 0)
            config->NUM_MINER = value;
        else if (strcmp(key, "TX_POOL_SIZE") == 0)
            config->TX_POOL_SIZE = value;
        else if (strcmp(key, "TRANSACTIONS_PER_BLOCK") == 0)
            config->TRANSACTIONS_PER_BLOCK = value;
        else if (strcmp(key, "BLOCKCHAIN_BLOCKS") == 0)
            config->BLOCKCHAIN_BLOCKS = value;
    }
    fclose(file);
}

// Função para criar IPCs
void create_ipcs() {

    // Criar a memória compartilhada
    shmid = shmget(SHM_KEY, sizeof(SharedMemory), IPC_CREAT | 0666);
    if (shmid < 0) {
        perror("shmget error\n");
        exit(1);
    }
    // Anexar a memória compartilhada
    shm = (SharedMemory *)shmat(shmid, NULL, 0);
   
}