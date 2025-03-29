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
#define MAX_TRANSACTIONS 100

// Estrutura para transações
typedef struct Configuration {
    int NUM_MINER;
    int TX_POOL_SIZE;
    int TRANSACTIONS_PER_BLOCK;
    int BLOCKCHAIN_BLOCKS;
    int TRANSACTION_POOL_SIZE;
} Config;



typedef struct Transaction {
    int id;
    char details[50];
} Transaction;

typedef struct SharedMemory {
    int transaction_count;
    Transaction transactions[MAX_TRANSACTIONS];
    pthread_mutex_t mutex;

} SharedMemory;


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



int main(int argc, char * argv[]){

	Config config;
	
	

    if(argc != 3){
        printf("Uso errado : <reward> <sleep time>");
        return -1;
    }

    
    int reward = atoi(argv[1]);
    
    int sleeptime = atoi(argv[2]);


    int shmid = shmget(SHM_KEY,sizeof(SharedMemory),0666);

    if(shmid < 0){
        perror("shmget error (TxGen)");
        exit(1);
    }
    
    //Anexar a memória compartilhada
    SharedMemory *shrd = (SharedMemory * )shmat(shmid,NULL,0);
    if (shrd == (void *)(-1)) {
        perror("shmat error");
        exit(1);
    }

    pthread_mutex_init(&(shrd->mutex), NULL);

    //Geração de transações
    int transaction_id = 1;  

    while (1) {
        pthread_mutex_lock(&(shrd->mutex));

        if (shrd->transaction_count < MAX_TRANSACTIONS) {
            Transaction new_tx;
            new_tx.id = transaction_id++;
            snprintf(new_tx.details, sizeof(new_tx.details), "Transaction %d - Reward: %d", new_tx.id, reward);

            //Guardar na memoria partilhada
            shrd->transactions[shrd->transaction_count] = new_tx;
            shrd->transaction_count++;

            printf("Transação gerada %d: %s\n", new_tx.id, new_tx.details);
        } 
        else {
            printf("Transaction buffer cheio. À espera...\n");
        }
        pthread_mutex_unlock(&(shrd->mutex));

        sleep(sleeptime);



    shmdt(shrd);
	}

    return 0;
}
