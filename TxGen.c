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

// Estrutura para transações
typedef struct Transaction {
    int id;
    char details[50];
} Transaction;

typedef struct SharedMemory {
    int transaction_count;
    pthread_mutex_t mutex;

} SharedMemory;


int main(int argc, char * argv[]){


    if(argc != 3){
        printf("Uso errado : <reward> <sleep time>");
        return -1;
    }

    
    int reward = atoi(argv[1]);
    
    int sleeptime = atoi(argv[2]);


    int shmid = shmget(SHM_KEY,sizeof(SharedMemory),0666);

    if(shmid < 0){
        perror("shmget error\n (TxGen)");
        exit(1);
    }
    
    //Anexar a memória compartilhada
    SharedMemory *shrd = (SharedMemory * )shmat(shmid,NULL,0);
    shrd = (SharedMemory *)shmat(shmid, NULL, 0);
    if (shrd == (SharedMemory *)(-1)) {
        perror("shmat error\n");
        exit(1);
    }

}