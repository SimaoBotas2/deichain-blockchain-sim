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
#include <openssl/sha.h>
#include <errno.h>
#include "structs.h"
#include "pow.h"


#define DEBUG // Remove esta linha para remover as mensagens de debug
#define SHM_KEY 0x1234 // Chave para segmento de memória compartilhado
#define BUFFER_SIZE 1000 //apenas temporário, mudar pra malloc dps
#define VALIDATOR_PIPE "/tmp/VALIDATOR_PIPE"

#define LEDGER_SHM_KEY 0x4321

// Variáveis globais

pthread_mutex_t mutex;
bool finish  = false;

FILE * file;

int shmid;
TransactionPool *shrd;
pthread_t *miner_threads = NULL; 
sem_t log_sem;
char msg[BUFFER_SIZE];

// varíaveis para a BlockChain Ledger
int ledger_shmid;
Blockchain *ldgr;

Config config;
int transactions_per_block;

//Váriaveis pra pids dos processos
pid_t pid_miner=-1;
pid_t pid_validator=-1;
pid_t pid_statistics=-1;


//Funcoes 

void controller();
void read_config(const char *filename);
void create_ipcs();
void *miner();
void *miner_action();
void *validator();
void *statistics();
void log_file(const char *message);
void cleanup();
void sigint_handler(int signum);
void miner_exit_handler(int sig);
bool validate_block(Block * block);



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


    //Finalizar aqui para tudo ficar documentado no log
    sem_destroy(&log_sem);
    fclose(file);

    return 0;
}

// Processo Controller
void controller() {
    
    signal(SIGINT,sigint_handler);

    sprintf(msg, "[CONTROLLER] Processo Controller começou (PID: %d)\n", getpid());
    log_file(msg);

    // Iniciar estrutura
    read_config("config.cfg");

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
        sprintf(msg,"[CONTROLLER] Processo Miner começou (PID: %d)\n", getpid());
        log_file(msg);
        #endif

        //ignorar o sinal, apenas o controller o vai ver
        signal(SIGINT,SIG_IGN);
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
        sprintf(msg,"[CONTROLLER] Processo Validator começou (PID: %d)\n", getpid());
        log_file(msg);
        #endif
        //ignorar o sinal, apenas o controller o vai ver
        signal(SIGINT,SIG_IGN);
        validator();
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
        sprintf(msg,"[CONTROLLER] Processo Statistics começou (PID: %d)\n", getpid());      
        log_file(msg);
        #endif
        //ignorar o sinal, apenas o controller o vai ver
        signal(SIGINT,SIG_IGN);
        statistics();
        exit(0);
    }

    waitpid(pid_miner, NULL, 0);
    waitpid(pid_validator, NULL, 0);
    waitpid(pid_statistics, NULL, 0);
    
   
    sprintf(msg,"[CONTROLLER] Processo Controller terminado após cleanup\n");      
    log_file(msg);

}

// Função para ler o arquivo de configuração
void read_config(const char *filename) {
    // Inicializa com valores inválidos
    config.NUM_MINER = -1;
    config.TX_POOL_SIZE = -1;
    config.TRANSACTIONS_PER_BLOCK = -1;
    config.BLOCKCHAIN_BLOCKS = -1;

    FILE *f = fopen(filename, "r");
    if (!f) {
        #ifdef DEBUG
        sprintf(msg, "Erro ao abrir arquivo de configuração\n");
        log_file(msg);
        #endif
        exit(1);
    }

    char key[BUFFER_SIZE];
    int value;

    while (fscanf(f, "%s - %d", key, &value) == 2) {
        if (strcmp(key, "NUM_MINERS") == 0 && value >= 0)
            config.NUM_MINER = value;
        else if (strcmp(key, "TX_POOL_SIZE") == 0 && value >= 0)
            config.TX_POOL_SIZE = value;
        else if (strcmp(key, "TRANSACTIONS_PER_BLOCK") == 0 && value >= 0)
            config.TRANSACTIONS_PER_BLOCK = value;
        else if (strcmp(key, "BLOCKCHAIN_BLOCKS") == 0 && value >= 0)
            config.BLOCKCHAIN_BLOCKS = value;
        else {
            sprintf(msg, "Erro ao atribuir um valor, verifique o valor de %s\n", key);
            log_file(msg);
        }
    }

    transactions_per_block = config.TRANSACTIONS_PER_BLOCK;

    fclose(f);

    // Verifica se algum campo obrigatório não foi atribuído
    if (config.NUM_MINER == -1 || config.TX_POOL_SIZE == -1 ||
        config.TRANSACTIONS_PER_BLOCK == -1 || config.BLOCKCHAIN_BLOCKS == -1) {
        #ifdef DEBUG
        sprintf(msg, "Configuração incompleta. Verifique se todos os campos estão definidos corretamente.\n");
        log_file(msg);
        #endif
        exit(1);
    }

}

// Função para criar IPCs
void create_ipcs() {

    //Transaction POOL início
   
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
    
    shrd->transaction_pending_set = 0;
    shrd->pool_size = config.TX_POOL_SIZE;
 
    //Inicializar todas as transaction entries vazias
    for(int i =0;i<shrd->pool_size;i++){
        shrd->entries[i].empty =true;
    }

       // Inicializar semáforo da transaction pool
       if(sem_init(&shrd->sem, 1, 1)==-1){
        #ifdef DEBUG
        sprintf(msg,"Erro ao criar semáforo da transaction pool\n");
        log_file(msg);
        #endif
    }

    //Transaction Pool Fim


    //BlockChain Ledger Inicio
    
    size_t ledger_size = sizeof(Blockchain) + (config.BLOCKCHAIN_BLOCKS * sizeof(Block));
    ledger_shmid = shmget(LEDGER_SHM_KEY, ledger_size, IPC_CREAT | 0666);
    if (ledger_shmid < 0){
        perror("shmget error\n");  
        exit(1);
    }
    
    ldgr = (Blockchain *)shmat(ledger_shmid, NULL, 0);
    if (ldgr == (Blockchain*)(-1)) {
        perror("shmat error\n");
        exit(1);
    }

    ldgr->max_blocks = config.BLOCKCHAIN_BLOCKS;

    ldgr->current_blocks = 0;
  
    ldgr->blocks = (Block*)((char*)ldgr + sizeof(Blockchain));

    if (sem_init(&ldgr->sem, 1, 1) == -1) {
        #ifdef DEBUG
        sprintf(msg, "Erro ao criar semáforo da blockchain\n");
        log_file(msg);
        #endif
    }

    // BlockChain Ledger Fim

    //Inicializar semáforo para acesso a config
    if(sem_init(&config.sem,1,1) ==-1){
        #ifdef DEBUG
        sprintf(msg,"Erro ao criar semáforo de acesso a Config\n");
        log_file(msg);
        #endif
    }


     // Criar Named Pipe para comunicação Miner -> Validator
     if (mkfifo(VALIDATOR_PIPE, 0666) == -1) {
        if (errno != EEXIST) {
            #ifdef DEBUG
            sprintf(msg,"Erro ao criar named pipe do validator: %s\n", strerror(errno));
            log_file(msg);
            #endif
            exit(1); // opcional, se for erro fatal
        }
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

   // Imprimir na tela 
   printf("[%02d-%02d-%04d %02d:%02d:%02d] %s",day, month, year, hours, minutes, seconds, message);

   sem_post(&log_sem);
}


// Processo Miner
void *miner(){

    sprintf(msg,"[MINER] Processo Miner inicializado\n");
    log_file(msg);


    //Sinal recebido do controler para terminar
    signal(SIGTERM, miner_exit_handler);

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

    sprintf(msg,"[MINER] Processo Miner terminado\n");
    log_file(msg);

    free(miner_threads);


    return NULL;
}

void miner_exit_handler(int sig) {
    //Avisar threads pra terminar
    pthread_mutex_lock(&mutex);
    finish = true;
    pthread_mutex_unlock(&mutex);
}

void *miner_action(void *arg) {
    int miner_id = *(int *)arg;
    char msg_local[BUFFER_SIZE];

    // Obter parâmetros de configuração
    sem_wait(&config.sem);
    int number_transactions = config.TRANSACTIONS_PER_BLOCK;
    int pool_size = config.TX_POOL_SIZE;
    sem_post(&config.sem);

    sprintf(msg_local, "[MINER] Thread miner %d inicializada\n", miner_id);
    log_file(msg_local);

    while (1) {
        pthread_mutex_lock(&mutex);
        if (finish) {
            pthread_mutex_unlock(&mutex);
            break;
        }
        pthread_mutex_unlock(&mutex);

        // Espera ter transações suficientes
        sem_wait(&shrd->sem);
        if (shrd->transaction_pending_set < number_transactions) {
            sem_post(&shrd->sem);
            sleep(1);
            continue;
        }

        // Coleta as transações
        Transaction txs[number_transactions];
        int idxs[number_transactions], collected = 0;
        for (int i = 0; i < pool_size && collected < number_transactions; i++) {
            if (!shrd->entries[i].empty) {
                txs[collected] = shrd->entries[i].tx;
                idxs[collected++] = i;
            }
        }

        // Marca como usadas

        
        /* Alteração para teste do validator
        for (int i = 0; i < collected; i++) {
            shrd->entries[idxs[i]].empty = true;
        }
        shrd->transaction_pending_set -= collected;

        */
        sem_post(&shrd->sem);

        // Monta o bloco
        Block block;
        block.transactions_count = number_transactions;
        block.timestamp = time(NULL);
        block.nonce = 0;
        block.miner_id  = miner_id;
        snprintf(block.id, TXB_ID_LEN, "Block-%d-%ld", miner_id, block.timestamp);

        // Aloca e copia transações
        block.transactions = malloc(number_transactions * sizeof(Transaction));
        if (!block.transactions) {
            log_file("[MINER] Erro malloc transactions\n");
            continue;
        }
        memcpy(block.transactions, txs, number_transactions * sizeof(Transaction));

        // Pega hash anterior
        sem_wait(&ldgr->sem);
        if (ldgr->current_blocks == 0) {
            strncpy(block.previous_hash, INITIAL_HASH, HASH_SIZE);
        } else {
            strncpy(block.previous_hash,
                    ldgr->blocks[ldgr->current_blocks - 1].hash,
                    HASH_SIZE);
        }
        sem_post(&ldgr->sem);

        // Proof of Work
        PoWResult result = proof_of_work(&block);
        if (result.error) {
            snprintf(msg_local, BUFFER_SIZE,
                     "[MINER] Thread %d: PoW falhou após %d ops\n",
                     miner_id, result.operations);
            log_file(msg_local);
            free(block.transactions);
            continue;
        }
        // Preenche hash final
        strncpy(block.hash, result.hash, HASH_SIZE);

        snprintf(msg_local, BUFFER_SIZE,
                 "[MINER] Thread %d: Bloco %s minerado! Nonce=%d, Hash=%s\n",
                 miner_id, block.id, block.nonce, block.hash);
        log_file(msg_local);

        // Envia ao Validator
        int fd = open(VALIDATOR_PIPE, O_WRONLY | O_NONBLOCK);
        if (fd == -1) {
            log_file("[MINER] Erro ao abrir pipe Validator\n");
            free(block.transactions);
            sleep(1);
            continue;
        }
        if (write(fd, &block, sizeof(Block)) != sizeof(Block)) {
            log_file("[MINER] Erro ao escrever no pipe Validator\n");
        } 
        else {
            sprintf(msg_local,"[MINER] Thread %d enviou bloco %s ao Validator\n",miner_id, block.id);
            log_file(msg_local);
        }
        close(fd);

        free(block.transactions);
        sleep(1);
    }

    sprintf(msg_local, "[MINER] Thread %d terminou\n", miner_id);
    log_file(msg_local);
    return NULL;
}

void *validator() {

    sprintf(msg,"[VALIDATOR] Processo Validator inicializado\n");
    log_file(msg);


    int fd;
    Block block;
    bool valid = false;

    fd = open(VALIDATOR_PIPE, O_RDONLY);
    if (fd == -1) {
        sprintf(msg,"[VALIDATOR] Erro ao Abrir Pipe\n");
        log_file(msg);
    }
    while (1) {
        ssize_t bytes = read(fd, &block, sizeof(Block));
        if (bytes == sizeof(Block)) {
            log_file("[VALIDATOR] Bloco recebido\n");

            valid = validate_block(&block);
            if(valid){
                //append_ledger(&block);
                log_file("[VALIDATOR] Bloco validado e adicionado ao ledger\n");
            }
            else {
                log_file("[VALIDATOR] Bloco inválido descartado\n");
            }
            }   
        else {
            log_file("[VALIDATOR] Erro ao ler o bloco do pipe");
            }
            
        } 
    
    

    close(fd);
    log_file("[VALIDATOR] Processo Validator terminado\n");

    return NULL;
}

bool validate_block(Block *block) {

    bool is_valid = true;
    return is_valid;
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

// Função que vai limpar todos os recursos utilizados
void cleanup() {


    // Semáforos
    if (ldgr) sem_destroy(&ldgr->sem);
    if (shrd) sem_destroy(&shrd->sem);

    // Memória compartilhada
    if (shrd) {
        shmdt(shrd);
        shrd = NULL;
    }
    if (ldgr) {
        shmdt(ldgr);
        ldgr = NULL;
    }
    if (shmid != -1) {
        shmctl(shmid, IPC_RMID, NULL);
        shmid = -1;
    }
    if (ledger_shmid != -1) {
        shmctl(ledger_shmid, IPC_RMID, NULL);
        ledger_shmid = -1;
    }

    //Validator Pipe
    if (unlink(VALIDATOR_PIPE) == -1) {
        #ifdef DEBUG
        sprintf(msg, "[CLEANUP] Erro ao remover pipe\n");
        log_file(msg);
        #endif
    }


    sprintf(msg,"[CLEANUP] Recursos limpos, a terminar programa\n");
    log_file(msg);

}

void sigint_handler(int signum){
    sprintf(msg,"[SIGNAL] ^C detetado, a limpar recursos\n");
    log_file(msg);

    if (pid_miner > 0){
    sprintf(msg,"[SIGNAL] A terminar Miner\n");
    log_file(msg);
    kill(pid_miner, SIGTERM);
    }
    if (pid_validator > 0) kill(pid_validator, SIGTERM);
    if (pid_statistics > 0) kill(pid_statistics, SIGTERM);

	cleanup();
}