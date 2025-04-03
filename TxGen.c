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

#define MAX_TRANSACTIONS 100 // Temporário


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

    int reward = atoi(argv[1]); //verificar intervalos

    if(reward <1 || reward >3){
        printf("Reward deve ser entre 1 e 3\n");
        return -1;
    }

    int sleeptime = atoi(argv[2]);

    if(sleeptime <200 || sleeptime >3000){
        printf("Sleeptime deve ser entre 200 e 3000 (ms)\n");
        return -1;
    }

    int shmid = shmget(SHM_KEY, sizeof(SharedMemory), 0666);
    if (shmid < 0) {
        perror("Erro shmget (TxGen)");
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

        sleep_ms(sleeptime);
    }

    shmdt(shrd);
	}

    return 0;
}
