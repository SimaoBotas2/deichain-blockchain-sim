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
#include <sys/wait.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <semaphore.h>
#include "structs.h"

#define DEBUG // Remove esta linha para remover as mensagens de debug
#define SHM_KEY 0x1234 // Chave para segmento de memória compartilhado
#define BUFFER_SIZE 100 //apenas temporário, mudar pra malloc dps
#define VALIDATOR_PIPE "VALIDATOR_PIPE"

// Variáveis globais

pthread_mutex_t mutex;
bool finish  = false;

FILE * file;

int shmid;
TransactionPool *shrd;
pthread_t *miner_threads = NULL; 
sem_t log_sem;
char msg[BUFFER_SIZE];


Config config;



//Funcoes 

void controller();
void read_config(const char *filename, Config *config);
void create_ipcs();
void *miner();
void *miner_action();
void *validator();
void *statistics();
void log_file(const char *message);
void cleanup();
void sigint_handler(int signum);



int main() {

    //Abrir ficheiro para log, para evitar abrir várias vezes
    file = fopen("DEIChain_log.txt", "a");
    if (file == NULL) {
        perror("[LOG FILE] Erro ao abrir arquivo de log\n");
        return -1;
    }


// Inicializar semáforo para o log
    if(sem_init(&log_sem,1,1)==-1){
        #ifdef DEBUG
        sprintf(msg,"Erro ao criar semáforo do log\n");
        log_file(msg);
        #endif
    }

    log_file("Simulação iniciada\n");
    controller();
    log_file("Simulação finalizada\n");
    return 0;
}

// Processo Controller
void controller() {

    sprintf(msg, "[CONTROLLER] Processo Controller começou (PID: %d)\n", getpid());
    log_file(msg);

    // Iniciar estrutura
    read_config("config.cfg", &config);

    #ifdef DEBUG
    log_file("Configurações carregadas:\n");
    sprintf(msg,"NUM_MINERS: %d\n", config.NUM_MINER);
    log_file(msg);
    sprintf(msg,"TX_POOL_SIZE: %d\n", config.TX_POOL_SIZE);
    log_file(msg);
    sprintf(msg,"TRANSACTIONS_PER_BLOCK: %d\n", config.TRANSACTIONS_PER_BLOCK);
    log_file(msg);
    sprintf(msg,"BLOCKCHAIN_BLOCKS: %d\n", config.BLOCKCHAIN_BLOCKS);
    log_file(msg);

    #endif

    create_ipcs();


    pid_t pid_miner, pid_validator, pid_statistics;
    
    pid_miner = fork();
    if (pid_miner < 0) {
        #ifdef DEBUG
        sprintf(msg,"Erro ao criar o processo miner\n");
        log_file(msg);
        #endif
        exit(1);
    } 
    else if (pid_miner == 0) {
        // Processo filho (Miner)
        #ifdef DEBUG
        sprintf(msg,"Processo Miner começou (PID: %d)\n", pid_miner);
        log_file(msg);
        #endif
        miner();
        exit(0);
    }

    pid_validator = fork();
    if (pid_validator < 0) {
        #ifdef DEBUG
        sprintf(msg,"Erro ao criar o processo validator\n");
        log_file(msg);
        #endif
        exit(1);
    }
    else if (pid_validator == 0) {
        // Processo filho (Validator)
        #ifdef DEBUG
        sprintf(msg,"Processo Validator começou (PID: %d)\n", pid_validator);
        log_file(msg);
        #endif
        validator(&config);
        exit(0);
    }

    pid_statistics = fork();
    if (pid_statistics < 0) {
        #ifdef DEBUG
        sprintf(msg,"Erro ao criar o processo statistics\n");
        log_file(msg);
        #endif
        exit(1);
    }
    else if (pid_statistics == 0) {
        // Processo filho (Statistics)
        #ifdef DEBUG
        sprintf(msg,"Processo Statistics começou (PID: %d)\n", pid_statistics);      
        log_file(msg);
        #endif
        statistics(&config);
        exit(0);
    }

    waitpid(pid_miner, NULL, 0);
    waitpid(pid_validator, NULL, 0);
    waitpid(pid_statistics, NULL, 0);
    
    /*// Processo pai continua sem esperar
    #ifdef DEBUG
    sprintf(msg,"Processo Controller (PID: %d) inicio corretamente\n", getpid());
    log_file(msg);
    #endif*/

}

// Função para ler o arquivo de configuração
void read_config(const char *filename, Config *config) {
    FILE file_config = fopen(filename, "r");
    if (!file) {
        #ifdef DEBUG
        sprintf(msg,"Erro ao abrir arquivo de configuração\n");
        log_file(msg);
        #endif
        exit(1);
    }

    char key[BUFFER_SIZE];
    int value;
    // Verifica se tem o nome do atributo e o seu devido valor
    // Fatla verificar os valores para ver se fazem sentido
    while (fscanf(file, "%s - %d", key, &value) == 2) {
        if (strcmp(key, "NUM_MINERS") == 0)
            config->NUM_MINER = value;
        else if (strcmp(key, "TRANSACTION_POOL_SIZE") == 0)
            config->TRANSACTION_POOL_SIZE = value;
        else if (strcmp(key, "TRANSACTIONS_PER_BLOCK") == 0)
            config->TRANSACTIONS_PER_BLOCK = value;
        else if (strcmp(key, "BLOCKCHAIN_BLOCKS") == 0)
            config->BLOCKCHAIN_BLOCKS = value;
        else if (strcmp(key, "TRANSACTION_POOL_SIZE") == 0)
            config->TRANSACTION_POOL_SIZE = value;
    }
    fclose(file_config);
}


// Função para criar IPCs
void create_ipcs() {

    //Transaction POOL início
    //Ainda por testar
   
    //Garantir que a memória alocada aguenta tudo
    size_t total_size = sizeof(TransactionPool) + (config.TX_POOL_SIZE* sizeof(TransactionEntry));

    // Criar a memória compartilhada da transaction pool
    shmid = shmget(SHM_KEY, total_size, IPC_CREAT | 0666);
    if (shmid < 0) {
        perror("shmget error\n");  
        exit(1);
    }
    // Anexar a memória compartilhada
    shrd = (TransactionPool *)shmat(shmid, NULL, 0);
    if (shrd == (TransactionPool*)(-1)) {
        perror("shmat error\n");
        exit(1);
    }
    
    shrd->entries = (TransactionEntry *)(shrd + 1); //alocar o vetor a seguir à main struct
    shrd->transaction_pending_set = 0;
    shrd->pool_size = config.TX_POOL_SIZE;
 
    //Inicializar todas as transaction entries vazias
    for(int i =0;i<shrd->pool_size;i++){
        shrd->entries[i].empty =true;
    }

    //Transaction Pool fim


    //Falta inicializar a memória do blockchain ledger

    // Inicializar semáforo da transaction pool
    if(sem_init(&shrd->sem, 1, 1)==-1){
        #ifdef DEBUG
        sprintf(msg,"Erro ao criar semáforo da transaction pool\n");
        log_file(msg);
        #endif
    }

    //Inicializar semáforo para acesso a config
    if(sem_init(&config.sem,1,1) ==-1){
        #ifdef DEBUG
        sprintf(msg,"Erro ao criar semáforo de acesso a Config\n");
        log_file(msg);
        #endif
    }


     // Criar Named Pipe para comunicação Miner -> Validator
     if (mkfifo(VALIDATOR_PIPE, 0666) == -1) {
            #ifdef DEBUG
            sprintf(msg,"Erro ao criar named pipe do validator\n");
            log_file(msg);
            #endif
    }

    //Criar Mutex para threads do miner
    int r = pthread_mutex_init(&mutex,NULL);
    if(r != 0){
        #ifdef DEBUG
            sprintf(msg,"Erro ao criar mutex das threads miner");
            log_file(msg);
            #endif
    }


    // Iniciar filas de mensagens, entre outros...
    
}

// Função para escrever no ficheiro .txt aquilo que acontece no código
void log_file(const char *message) {
    
    sem_wait(&log_sem);
   // Obter data e hora atual
   time_t now = time(NULL);
   struct tm *t = localtime(&now);

    int year = t->tm_year + 1900; // Desde 1900
    int month = t->tm_mon + 1; // o mês vai de 0 a 11
    int day = t->tm_mday;

    int hours = t->tm_hour;
    int minutes = t->tm_min;
    int seconds = t->tm_sec;

    // Escreve no ficheiro 
   fprintf(file, "[%02d-%02d-%04d %02d:%02d:%02d] %s", day, month, year, hours, minutes, seconds, message);
   
   fclose(file);

   // Imprimir na tela 
   printf("[%02d-%02d-%04d %02d:%02d:%02d] %s",day, month, year, hours, minutes, seconds, message);

   sem_post(&log_sem);
}


// Processo Miner
void *miner(){

    int i;
    sem_wait(&config.sem);
    int NUM_MINER = config.NUM_MINER;
    sem_post(&config.sem);

    miner_threads = malloc(NUM_MINER * sizeof(pthread_t)); //possivel solução para alocar previamente e ser global
    int miner_ids[NUM_MINER];

    for(i = 0;i<NUM_MINER;i++){
        miner_ids[i] = i;
        if (pthread_create(&miner_threads[i],NULL,miner_action,&miner_ids[i]) != 0) {
            #ifdef DEBUG
            sprintf(msg,"[MINER] Erro ao criar Miner Thread %d\n",miner_ids[i]);   
            log_file(msg);
            #endif
            exit(1);
        }
    }

    for (i = 0; i < NUM_MINER; i++) {
        if (pthread_join(miner_threads[i],NULL) != 0) {
            #ifdef DEBUG
            sprintf(msg,"[MINER] Erro ao juntar Miner Thread %d\n",miner_ids[i]);
            log_file(msg);
            #endif
            exit(1);
        }
    }

    return NULL;
}

void *miner_action(void *arg) {

    int miner_id = *(int *)arg;

    char msg_local[BUFFER_SIZE];

 

    //Criar o bloco para guardar as transações

    /*
    Código do bloco 
    
    */

    sprintf(msg_local,"[MINER] Thread %d inicializada\n",miner_id);

    log_file(msg_local);


    pthread_mutex_lock(&mutex);
    int number_transactions = config.TRANSACTIONS_PER_BLOCK;
    int pool_size = config.TX_POOL_SIZE;
    pthread_mutex_unlock(&mutex);


    while (1) { //adicionar variavel de sincronização para parar a thread caso receba sinal
    
        pthread_mutex_lock(&mutex);

        if(finish){ //variavel para controle das threads, usada pra sincronização e cleanup
            pthread_mutex_unlock(&mutex);
            break;
        }

        //Se na transaction pool nao houver transações suficientes, esperar para o proximo loop
        if (shrd->transaction_pending_set < number_transactions) {
            pthread_mutex_unlock(&mutex);
            sleep(1);
            continue; 
        }

        //Código de execução da thread

        int entry_id;

        for(int i =0;i<number_transactions;i++){
            entry_id = rand() % pool_size;
            while(shrd->entries[entry_id].empty){
                entry_id = rand() % pool_size;
            }

            sprintf(msg_local,"[MINER] Thread %d a processar a transação %d",miner_id, shrd->entries[entry_id].tx.id);
            log_file(msg_local);

            //POW da transação


            //sucesso
            printf("[MINER] Thread %d minerou com sucessou a transação %d\n", miner_id, shrd->entries[entry_id].tx.id);
            log_file(msg_local);


            //Guardar a transação no bloco

        }

        //Enviar o bloco através de um named pipe para o validator





        pthread_mutex_unlock(&mutex);

        //Depois alterar isto para dar match à nova transaction structure
        
        //printf("[MINER] Thread %d a processar transação %d: %s\n", miner_id, tx[0].id, tx[0].details);

        //printf("[MINER] Thread %d minerou com sucessou a transação %d\n", miner_id, tx[0].id);
    }

    sprintf(msg_local,"Miner %d terminou\n",miner_id);
    log_file(msg_local);
    pthread_exit(NULL);

}

void *validator() {

    sprintf(msg,"[VALIDATOR] Processo Validator inicializado\n");
    log_file(msg);

    /*while (1) {
        #ifdef DEBUG
        //log_file("A funcionar...\n");
        sleep(1);
        #endif
    
        Code...
    }*/
    
    log_file("[VALIDATOR] Processo Validator terminado\n");

    return NULL;
}

void *statistics() {

    sprintf(msg,"[STATISTICS] Processo Statistics inicializado\n");
    log_file(msg);

    /*for(i = 0; i < 5; i++) {
        #ifdef DEBUG
        log_file("A funcionar...\n");
        sleep(1);
        #endif
    
        Code...
    }*/
    
    log_file("[STATISTICS] Processo Statistics terminado\n");

    return NULL;
}

void cleanup(){
//Função que vai limpar todos os recursos utilizados

    pthread_mutex_lock(&mutex);
    finish = true;
    pthread_mutex_unlock(&mutex);
    free(miner_threads);

    sem_destroy(&shrd->sem);

    //Eliminar Transaction Pool Memory
    if(shrd != NULL){
        shmdt(shrd);
        shrd = NULL;
    }

    if(shmid == -1){
        shmctl(shmid,IPC_RMID,NULL);
        shmid = -1;
    }

    //Destruir mutex (é preciso confirmar??), visto que deixamos a thread acabar ?
    pthread_mutex_destroy(&mutex);


    log_file("Recursos Eliminados com sucesso!\n");
    //Apenas eliminar este semáforo depois para evitar erros do log
    sem_destroy(&log_sem);

    //fechar ficheiro da config
    fclose(file);

}


void sigint_handler(int signum){
    sprintf(msg,"SInal ^C detetado, a limpar recursos\n");
    log_file(msg);
	printf("\n\n^C pressionado. A limpar recursos\n");
	cleanup();
	printf("Recursos limpos, programa a finalizar\n");
	exit(0);
}