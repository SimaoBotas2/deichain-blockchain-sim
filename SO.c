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
#include <stddef.h>
#include "structs.h"
#include "pow.h"


// #define DEBUG // Remove esta linha para remover as mensagens de debug
#define SHM_KEY 0x1234 // Chave para segmento de memória compartilhado
#define BUFFER_SIZE 1000 //apenas temporário, mudar pra malloc dps
#define VALIDATOR_PIPE "/tmp/validator_pipe" // Nome do pipe

#define LEDGER_SHM_KEY 0x4321

// Variáveis globais

//Controlo das threads
pthread_mutex_t mutex;
pthread_mutex_t pipe_mutex;

bool finish_miner  = false;
bool finish_validator = false;
bool finish_statistics = false;


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


//Váriaveis para estatísticas
int *valid_blocks;
int *invalid_blocks;
int *credits_by_miner;

int total_blocks = 0;
int total_valid = 0;
double total_verification_time = 0.0;
int verified_count = 0;

int msqid;

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
bool validate_transaction(const Transaction *tx);
bool is_tx_confirmed(const char *tx_id);
bool validate_block(Block *block);
void append_ledger(const Block *block);
void remove_transactions(const Block *block);
void return_transactions(const Block *block);
void validator_exit_handler(int signum);
void statistics_exit_handler(int signum);
void statistics_usr1_handler(int sigum);



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
    
    controller();

    //Finalizar aqui para tudo ficar documentado no log
    sem_destroy(&log_sem);
    fclose(file);

    return 0;
}

// Processo Controller
void controller() {
    
    signal(SIGINT,sigint_handler);

    log_file("[CONTROLLER] Simulação começou\n");

    // Iniciar estrutura
    read_config("config.cfg");

    #ifdef DEBUG
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
        sprintf(msg,"[CONTROLLER] Processo Miner começou (PID: %d)\n", getpid());
        log_file(msg);

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
        sprintf(msg,"[CONTROLLER] Processo Validator começou (PID: %d)\n", getpid());
        log_file(msg);

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
        sprintf(msg,"[CONTROLLER] Processo Statistics começou (PID: %d)\n", getpid());      
        log_file(msg);
        //ignorar o sinal, apenas o controller o vai ver
        signal(SIGINT,SIG_IGN);
        statistics();
        exit(0);
    }

    waitpid(pid_miner, NULL, 0);
    waitpid(pid_validator, NULL, 0);
    waitpid(pid_statistics, NULL, 0);
    
   
    log_file("[CONTROLLER] Simulação terminou\n");      
 
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
            log_file("Erro ao atribuir valores de configuração, verifique o valores do config.cfg\n");
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

    //Sinal recebido do controller para terminar
    signal(SIGTERM, miner_exit_handler);

    int i;
    sem_wait(&config.sem);
    int NUM_MINER = config.NUM_MINER;
    sem_post(&config.sem);

    miner_threads = malloc(NUM_MINER * sizeof(pthread_t)); 
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
    finish_miner = true;
    pthread_mutex_unlock(&mutex);
}

void *miner_action(void *arg) {
    int miner_id = *(int*)arg;
    char buf[BUFFER_SIZE];

    sem_wait(&config.sem);
    int num_txs = config.TRANSACTIONS_PER_BLOCK;
    int pool_sz = config.TX_POOL_SIZE;
    sem_post(&config.sem);

    snprintf(buf, sizeof buf, "[MINER] Thread %d inicializada\n", miner_id);
    log_file(buf);

    while (1) {
        pthread_mutex_lock(&mutex);
        if (finish_miner) { 
            pthread_mutex_unlock(&mutex);
            break; 
        }
        pthread_mutex_unlock(&mutex);

        sem_wait(&shrd->sem);
        if (shrd->transaction_pending_set < num_txs) {
            sem_post(&shrd->sem);
            sleep(1);
            continue;
        }

        Transaction txs[num_txs];
        int ages[num_txs];
        int collected_indices[num_txs];
        int collected = 0;

        // Coleta as transações mais lucrativas
        for (int i = 0; i < pool_sz; i++) {
            if (!shrd->entries[i].empty) {
                if (collected < num_txs) {
                    txs[collected] = shrd->entries[i].tx;
                    ages[collected] = shrd->entries[i].age;
                    collected_indices[collected] = i;
                    collected++;
                } else {
                    int min_idx = 0;
                    for (int j = 1; j < num_txs; j++) {
                        if (txs[j].reward < txs[min_idx].reward || (txs[j].reward == txs[min_idx].reward && ages[j] < ages[min_idx])) {
                            min_idx = j;
                        }
                    }
                    if (shrd->entries[i].tx.reward > txs[min_idx].reward ||
                        (shrd->entries[i].tx.reward == txs[min_idx].reward && 
                         shrd->entries[i].age > ages[min_idx])) {
                        txs[min_idx] = shrd->entries[i].tx;
                        ages[min_idx] = shrd->entries[i].age;
                        collected_indices[min_idx] = i;
                    }
                }
            }
        }

        // Remove as transações coletadas do pool
        for (int j = 0; j < num_txs; j++) {
            shrd->entries[collected_indices[j]].empty = true;
        }
        shrd->transaction_pending_set -= num_txs;
        sem_post(&shrd->sem);

        // Prepara o bloco
        size_t header_sz = offsetof(Block, transactions);
        size_t txs_sz = num_txs * sizeof(Transaction);
        size_t total_sz = header_sz + txs_sz;

        Block *blk = malloc(total_sz);
        if (!blk) {
            log_file("[MINER] malloc falhou\n");
            continue;
        }

        blk->transactions_count = num_txs;
        blk->timestamp = time(NULL);
        blk->nonce = 0;
        blk->miner_id = miner_id;
        snprintf(blk->id, TXB_ID_LEN, "Block-%d-%ld", miner_id, blk->timestamp);
        memcpy(blk->transactions, txs, txs_sz);

        // Obtém o previous_hash do ledger
        sem_wait(&ldgr->sem);
        if (ldgr->current_blocks == 0) {
            strncpy(blk->previous_hash, INITIAL_HASH, HASH_SIZE);
        } else {
            strncpy(blk->previous_hash,
                    ldgr->blocks[ldgr->current_blocks - 1].hash,
                    HASH_SIZE);
        }
        sem_post(&ldgr->sem);

        // Executa PoW
        PoWResult res = proof_of_work(blk);
        if (res.error) {
            snprintf(buf, sizeof buf, "[MINER] PoW falhou após %d ops\n", res.operations);
            log_file(buf);
            free(blk);
            continue;
        }
        strncpy(blk->hash, res.hash, HASH_SIZE);

        // Envia o bloco para o validador
        pthread_mutex_lock(&pipe_mutex); // Mutex específico para o pipe
        int fd = open(VALIDATOR_PIPE, O_WRONLY);
        if (fd == -1 || write(fd, blk, total_sz) != (ssize_t)total_sz) {
            #ifdef DEBUG 
            log_file("[MINER] Erro ao escrever no pipe\n");
            #endif
        } else {
            sprintf(buf, "[MINER] Bloco %s enviado ao Validator\n", blk->id);
            log_file(buf);
        }
        if (fd != -1) close(fd);
        pthread_mutex_unlock(&pipe_mutex);

        free(blk);
    }

    sprintf(buf, "[MINER] Thread %d terminou\n", miner_id);
    log_file(buf);
    return NULL;
}


void validator_exit_handler(int signum){
    finish_validator = true;
}

void *validator() {
    signal(SIGTERM, validator_exit_handler);
    log_file("[VALIDATOR] Processo Iniciado\n");

    int fd = open(VALIDATOR_PIPE, O_RDONLY);
    if (fd < 0) {
        #ifdef DEBUG
        log_file("[VALIDATOR] Erro ao abrir pipe\n");
        #endif
        return NULL;
    }

    const size_t header_sz = offsetof(Block, transactions);
    while (!finish_validator) {
        // 1) lê só o cabeçalho
        Block header;
        ssize_t r = read(fd, &header, header_sz);
        if (r <= 0) {
            if (errno == EINTR) break;
            sleep(1);
            continue;
        }
        if ((size_t)r != header_sz) {
            log_file("[VALIDATOR] Header incompleto\n");
            continue;
        }

        // Aloca o bloco completo
        size_t txs_sz = header.transactions_count * sizeof(Transaction);
        size_t total_sz = header_sz + txs_sz;
        Block *blk = malloc(total_sz);
        if (!blk) {
            #ifdef DEBUG
            log_file("[VALIDATOR] Malloc para o Bloco Falhou\n");
            #endif
            continue;
        }
        memcpy(blk, &header, header_sz);

        sprintf(msg,"[VALIDATOR] Bloco recebido do Miner : %d\n",blk->miner_id);
        log_file(msg);

        // Lê as transações
        r = read(fd, blk->transactions, txs_sz);
        if ((size_t)r != txs_sz) {
            #ifdef DEBUG
            log_file("[VALIDATOR] Transações incompletas\n");
            free(blk);
            #endif
            continue;
        }

        bool is_valid = validate_block(blk);

        // Validação e append do bloco
        if (is_valid) {
            remove_transactions(blk);
            sprintf(msg,"[VALIDATOR] Bloco do Miner %d Aceite e Enviado para o Ledger\n",blk->miner_id);
            log_file(msg);
        }
        else {
            return_transactions(blk);
            log_file("[VALIDATOR] Bloco Rejeitado\n");
        }

        key_t key = ftok("/tmp", 'S');
        int msqid = msgget(key, 0666 | IPC_CREAT);
        if (msqid == -1) {
            log_file("[VALIDATOR] Erro ao aceder à message queue\n");
        } 
        else {
            StatMessage smsg;
            smsg.mtype = STATS_MTYPE;
            smsg.miner_id = blk->miner_id;
            smsg.valid = is_valid;
            // calcular créditos (só se válido)
            smsg.credits = 0;
            for (int i = 0; i < blk->transactions_count; i++) {
                if (smsg.valid) {
                    smsg.credits += blk->transactions[i].reward;
                }
            }

            // obter timestamp mais antigo
            time_t min_time = blk->transactions[0].timestamp;
            for (int i = 1; i < blk->transactions_count; i++) {
                if (blk->transactions[i].timestamp < min_time) {
                    min_time = blk->transactions[i].timestamp;
                }
            }

            smsg.tx_start_time = min_time;
            smsg.block_time = blk->timestamp;

            // enviar mensagem
            if (msgsnd(msqid, &smsg, sizeof(StatMessage) - sizeof(long), 0) == -1) {
                log_file("[VALIDATOR] Erro ao enviar mensagem para Statistics\n");
          }
        }

        free(blk);
    }

    close(fd);
    log_file("[VALIDATOR] Processo Terminado\n");
    return NULL;
}

bool validate_block(Block *block) {
    // Verifica PoW
    if (verify_nonce(block) == 0) {
        log_file("[VALIDATOR] PoW inválido\n");
        return false;
    }

    // Checa por transações repetidas dentro do bloco
    for (int i = 0; i < block->transactions_count; ++i) {
        for (int j = i + 1; j < block->transactions_count; ++j) {
            if (strcmp(block->transactions[i].id,
                       block->transactions[j].id) == 0) {
                char buf[128];
                sprintf(buf,"[VALIDATOR] Bloco inválido: transação repetida ID=%s\n",block->transactions[i].id);
                log_file(buf);
                return false;
            }
        }
    }

    // Valida cada transação
    for (int i = 0; i < block->transactions_count; ++i) {
        if (!validate_transaction(&block->transactions[i])) {
            char buf[128];
            sprintf(buf,
                    "[VALIDATOR] Transação %d inválida: ID=%s\n",
                    i, block->transactions[i].id);
            log_file(buf);
            return false;
        }
    }

    // check de cadeia de hashes e append ao ledger
    sem_wait(&ldgr->sem);

    // Verifica encadeamento de hashes
    if (ldgr->current_blocks > 0) {
        Block *last = &ldgr->blocks[ldgr->current_blocks - 1];
        if (strncmp(block->previous_hash, last->hash, HASH_SIZE) != 0) {
            sem_post(&ldgr->sem);
            log_file("[VALIDATOR] Hash não combina com anterior\n");
            return false;
        }
    }

    // Append ao ledger
    if (ldgr->current_blocks < ldgr->max_blocks) {
        ldgr->blocks[ldgr->current_blocks] = *block;
        ldgr->current_blocks++;
    } 
    else {
        log_file("[VALIDATOR] Ledger cheio, bloco ignorado\n");
    }

    sem_post(&ldgr->sem);
    return true;
}

void return_transactions(const Block *block) {
    //Retorna transações que já tinham sido usadas noutro bloco para a transaction pool
    sem_wait(&shrd->sem);
    for (int i = 0; i < block->transactions_count; ++i) {
        const Transaction *tx = &block->transactions[i];
        if (is_tx_confirmed(tx->id)) {
            continue;
        }
        for (int j = 0; j < shrd->pool_size; ++j) {
            if (shrd->entries[j].empty) {
                shrd->entries[j].tx = *tx;
                shrd->entries[j].empty = false;
                shrd->entries[j].age ++; //aumenta a age quando a tx volta à pool
                if(shrd->entries[j].age % 50 == 0){
                    shrd->entries[j].tx.reward++; //aumenta a reward dado a idade da tx
                }
                shrd->transaction_pending_set++;
                break;
            }
        }
    }
    sem_post(&shrd->sem);
}

void remove_transactions(const Block *block) {
    //Remove as transações que já foram validadas num bloco da Transaction Pools
    sem_wait(&shrd->sem);
    for (int i = 0; i < block->transactions_count; ++i) {
        const char *txid = block->transactions[i].id;
        for (int j = 0; j < shrd->pool_size; ++j) {
            if (!shrd->entries[j].empty &&
                strcmp(shrd->entries[j].tx.id, txid) == 0) {
                shrd->entries[j].empty = true;
                shrd->transaction_pending_set--;
                break;
            }
        }
    }
    sem_post(&shrd->sem);
}

bool validate_transaction(const Transaction *tx) {
    // Verifica se a transação já existe na blockchain (foi confirmada)
    if (is_tx_confirmed(tx->id)) {
        char buffer[128];
        sprintf(buffer,"[VALIDATOR] TX duplicada no ledger: %s\n", tx->id);
        log_file(buffer);
        return false;
    }

    // Verifica se reward ou value são negativos
    if (tx->reward < 0 || tx->value < 0) {
        log_file("[VALIDATOR] TX valores negativos\n");
        return false;
    }

    // Verifica se o tempo é válido
    if (tx->timestamp == 0 || tx->timestamp > time(NULL)) {
        log_file("[VALIDATOR] TX com timestamp inválido\n");
        return false;
    }

    // Verifica se o ID está vazio
    if (tx->id[0] == '\0') {
        log_file("[VALIDATOR] TX sem ID\n");
        return false;
    }

    return true;
}

bool is_tx_confirmed(const char *tx_id) {
    //Procura no ledger se a transição existe
    bool found = false;
    sem_wait(&ldgr->sem);
    for (int b = 0; b < ldgr->current_blocks; ++b) {
        Block *blk = &ldgr->blocks[b];
        for (int t = 0; t < blk->transactions_count; ++t) {
            if (strcmp(blk->transactions[t].id, tx_id) == 0) {
                found = true;
                break;
            }
        }
        if (found) break;
    }
    sem_post(&ldgr->sem);
    return found;
}

void statistics_exit_handler(int signum){
    finish_statistics = true;
}

void *statistics() {

    signal(SIGTERM, statistics_exit_handler);
    signal(SIGUSR1, statistics_usr1_handler);

    sem_wait(&config.sem);
    int NUM_MINER = config.NUM_MINER;
    sem_post(&config.sem);


    valid_blocks = malloc(NUM_MINER * sizeof(int));
    invalid_blocks = malloc(NUM_MINER * sizeof(int));
    credits_by_miner = malloc(NUM_MINER * sizeof(int));

    if (!valid_blocks || !invalid_blocks || !credits_by_miner) {
        log_file("[STATISTICS] Erro ao alocar memória para estatísticas\n");
        pthread_exit(NULL);
    }

    for (int i = 0; i < NUM_MINER; i++) {
        valid_blocks[i] = 0;
        invalid_blocks[i] = 0;
        credits_by_miner[i] = 0;
    }

    sprintf(msg,"[STATISTICS] Processo Statistics inicializado (PID: %d)\n", getpid());
    log_file(msg);

    // Criar fila de mensagens
    key_t key = ftok("/tmp", 'S');
    msqid = msgget(key, 0666 | IPC_CREAT);
    if (msqid == -1) {
        #ifdef DEBUG
        log_file("[STATISTICS] Erro ao criar/aceder à message queue\n");
        #endif
        free(valid_blocks);
        free(invalid_blocks);
        free(credits_by_miner);
        pthread_exit(NULL);
    }

    StatMessage smsg;

    while (!finish_statistics) {
        ssize_t r = msgrcv(msqid, &smsg, sizeof(StatMessage) - sizeof(long), 0, IPC_NOWAIT);
        if (r > 0) {
            int id = smsg.miner_id;
            total_blocks++;

            if (smsg.valid) {
                valid_blocks[id]++;
                credits_by_miner[id] += smsg.credits;
                total_valid++;

                double duration = difftime(smsg.block_time, smsg.tx_start_time);
                if (duration >= 0) {
                    total_verification_time += duration;
                    verified_count++;
                } else {
                    log_file("[STATISTICS] Erro: duração negativa calculada para verificação\n");
                }
            } else {
                invalid_blocks[id]++;
            }
        } else {
            usleep(200000); // Evitar busy waiting
        }
    }

    log_file("[STATISTICS] Processo Statistics a terminar...\n");
    statistics_usr1_handler(SIGUSR1); // Imprime estatísticas finais

    free(valid_blocks);
    free(invalid_blocks);
    free(credits_by_miner);
}

void statistics_usr1_handler(int sigum) {
    log_file("[STATISTICS] Sinal SIGUSR1 recebido. Estatísticas atuais:\n");

    for (int i = 0; i < config.NUM_MINER; i++) {
        if (valid_blocks[i] || invalid_blocks[i]) {
            sprintf(msg, "Miner %d - Válidos: %d | Inválidos: %d | Créditos: %d\n",
                    i, valid_blocks[i], invalid_blocks[i], credits_by_miner[i]);
            log_file(msg);
        }
    }

    sprintf(msg, "Total de blocos recebidos: %d\n", total_blocks);
    log_file(msg);
    sprintf(msg, "Total de blocos válidos: %d\n", total_valid);
    log_file(msg);
    // Verificar se verified_count > 0 antes de calcular o tempo médio
    if (verified_count > 0) {
        double  avg_verification_time = total_verification_time / verified_count;
        sprintf(msg, "Tempo médio de verificação: %.2f segundos\n", avg_verification_time);
        log_file(msg);
    } else {
        log_file("Tempo médio de verificação: Não disponível (nenhum bloco verificado)\n");
    }
}

void cleanup() {
    // Função que vai limpar todos os recursos utilizados

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
    if (pid_validator > 0){ 
    sprintf(msg,"[SIGNAL] A terminar Validator\n");
    log_file(msg);
    kill(pid_validator, SIGTERM);
    };
    if (pid_statistics > 0){
    sprintf(msg,"[SIGNAL] A terminar Statistics\n");
    log_file(msg);
    kill(pid_statistics, SIGTERM);
    }

	cleanup();
}