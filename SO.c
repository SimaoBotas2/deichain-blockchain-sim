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
    int TRANSACTION_POOL_SIZE;
} Config;

// Estrutura para transações
typedef struct Transaction {
    int id;
    char details[50];
} Transaction;

typedef struct SharedMemory {
    Transaction transactions[100];   //Temporario
    int transaction_count;

} SharedMemory;

void controller();
void read_config(const char *filename, Config *config);
void create_ipcs();
void * miner(Config * config);
void * miner_action();


// Variáveis globais
int shmid;

SharedMemory *shrd;


int main() {
    controller();
    return 0;
}


//Processo Miner

void * miner(Config * config){

    int i =0;

    Config * c = config;

    int NUM_MINER = c->NUM_MINER;

    pthread_t miner_threads[config.NUM_MINER];
    int ids[NUM_MINER];

    for(i=0;i<NUM_MINER;i++){
        ids[i] = i;
        pthread_create(&miner_threads[i],NULL,miner_action,&ids[i]);
        }

    for (i = 0; i < NUM_MINER; i++) {
        pthread_join(miner_threads[i],NULL);
        }


    return NULL;
}


    void *miner_action(void *arg) {
        int miner_id = *(int *)arg;
    
        while (1) {
            pthread_mutex_lock(&shrd->mutex);
    
            if (shrd->transaction_count == 0) {
                pthread_mutex_unlock(&shrd->mutex);
                break;  // Stop if no transactions are left
            }
    
            Transaction tx = shrd->transactions[0];
    
            for (int i = 0; i < shrd->transaction_count - 1; i++) {
                shrd->transactions[i] = shrd->transactions[i + 1];
            }
            shrd->transaction_count++;
    
            pthread_mutex_unlock(&shrd->mutex);
    
        
            printf("Miner %d a processar transação %d: %s\n", miner_id, tx.id, tx.details);
            sleep(1); 
    
            printf("Miner %d minerou com sucessou a transação %d\n", miner_id, tx.id);
        }
    
        return NULL;
    }



// Função Controller
void controller() {

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
    miner(&config);
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
    shrd = (SharedMemory *)shmat(shmid, NULL, 0);
    if (shrd == (SharedMemory *)(-1)) {
        perror("shmat error\n");
        exit(1);
    }
    
    // Iniciar o mutex na memória compartilhada
    pthread_mutex_init(&shrd->mutex, NULL);

    // Inicia o counter de transações da memória partilhada
    shrd->transaction_count = 0; 

}

void add_transaction(Transaction t) {

    pthread_mutex_lock(&shrd->mutex);



    pthread_mutex_unlock(&shrd->mutex);
} 

