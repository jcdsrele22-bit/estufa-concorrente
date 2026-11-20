#include <errno.h>
#include <fcntl.h>
#include <mqueue.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define FILA "/estufa_mensagens"
#define TAM_BUFFER 4
#define LIMIAR_LIGA 35
#define LIMIAR_DESLIGA 50

typedef struct {
    int leitura;
    int umidade;
    int encerrar;
} Mensagem;

typedef struct {
    Mensagem dados[TAM_BUFFER];
    int inicio;
    int fim;
} Buffer;

static Buffer buffer;
static sem_t espacos_livres;
static sem_t itens_disponiveis;
static pthread_mutex_t mutex_buffer = PTHREAD_MUTEX_INITIALIZER;

static pthread_mutex_t mutex_estado = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond_estado = PTHREAD_COND_INITIALIZER;
static int irrigacao_ligada = 0;
static int versao_estado = 0;
static int controlador_finalizado = 0;

static void inserir_no_buffer(Mensagem mensagem) {
    sem_wait(&espacos_livres);
    pthread_mutex_lock(&mutex_buffer);

    buffer.dados[buffer.fim] = mensagem;
    buffer.fim = (buffer.fim + 1) % TAM_BUFFER;

    pthread_mutex_unlock(&mutex_buffer);
    sem_post(&itens_disponiveis);
}

static Mensagem retirar_do_buffer(void) {
    sem_wait(&itens_disponiveis);
    pthread_mutex_lock(&mutex_buffer);

    Mensagem mensagem = buffer.dados[buffer.inicio];
    buffer.inicio = (buffer.inicio + 1) % TAM_BUFFER;

    pthread_mutex_unlock(&mutex_buffer);
    sem_post(&espacos_livres);

    return mensagem;
}

static int executar_sensor(void) {
    const int leituras[] = {68, 47, 33, 29, 40, 51, 46, 32, 56, 70};
    const int quantidade = sizeof(leituras) / sizeof(leituras[0]);

    mqd_t fila = mq_open(FILA, O_WRONLY);
    if (fila == (mqd_t)-1) {
        perror("sensor: mq_open");
        return 1;
    }

    printf("[Sensor] Simulando leituras de umidade do solo.\n");

    for (int i = 0; i < quantidade; i++) {
        Mensagem mensagem = {
            .leitura = i + 1,
            .umidade = leituras[i],
            .encerrar = 0
        };

        if (mq_send(fila, (const char *)&mensagem, sizeof(mensagem), 0) == -1) {
            perror("sensor: mq_send");
            mq_close(fila);
            return 1;
        }

        printf("[Sensor] Leitura %d enviada: %d%%\n",
               mensagem.leitura, mensagem.umidade);
        usleep(700000);
    }

    Mensagem fim = {.leitura = 0, .umidade = 0, .encerrar = 1};
    mq_send(fila, (const char *)&fim, sizeof(fim), 0);

    mq_close(fila);
    return 0;
}

static void *receber_leituras(void *arg) {
    (void)arg;
    mqd_t fila = mq_open(FILA, O_RDONLY);

    if (fila == (mqd_t)-1) {
        perror("receptor: mq_open");
        Mensagem fim = {.encerrar = 1};
        inserir_no_buffer(fim);
        return NULL;
    }

    while (1) {
        Mensagem mensagem;
        ssize_t tamanho = mq_receive(
            fila, (char *)&mensagem, sizeof(mensagem), NULL
        );

        if (tamanho == -1) {
            if (errno == EINTR) {
                continue;
            }
            perror("receptor: mq_receive");
            break;
        }

        inserir_no_buffer(mensagem);

        if (mensagem.encerrar) {
            break;
        }
    }

    mq_close(fila);
    return NULL;
}

static void *controlar_irrigacao(void *arg) {
    (void)arg;

    while (1) {
        Mensagem mensagem = retirar_do_buffer();

        if (mensagem.encerrar) {
            pthread_mutex_lock(&mutex_estado);
            controlador_finalizado = 1;
            pthread_cond_broadcast(&cond_estado);
            pthread_mutex_unlock(&mutex_estado);
            break;
        }

        printf("[Controle] Leitura %d: umidade em %d%%\n",
               mensagem.leitura, mensagem.umidade);

        pthread_mutex_lock(&mutex_estado);
        int estado_anterior = irrigacao_ligada;

        if (!irrigacao_ligada && mensagem.umidade < LIMIAR_LIGA) {
            irrigacao_ligada = 1;
        } else if (irrigacao_ligada &&
                   mensagem.umidade >= LIMIAR_DESLIGA) {
            irrigacao_ligada = 0;
        }

        if (estado_anterior != irrigacao_ligada) {
            versao_estado++;
            pthread_cond_signal(&cond_estado);
        }
        pthread_mutex_unlock(&mutex_estado);
    }

    return NULL;
}

static void *monitorar_estado(void *arg) {
    (void)arg;
    int ultima_versao = 0;

    pthread_mutex_lock(&mutex_estado);

    while (1) {
        while (ultima_versao == versao_estado &&
               !controlador_finalizado) {
            pthread_cond_wait(&cond_estado, &mutex_estado);
        }

        if (ultima_versao != versao_estado) {
            ultima_versao = versao_estado;
            int estado = irrigacao_ligada;
            pthread_mutex_unlock(&mutex_estado);

            printf("[Monitor] Irrigação %s.\n",
                   estado ? "LIGADA" : "DESLIGADA");

            pthread_mutex_lock(&mutex_estado);
        } else if (controlador_finalizado) {
            break;
        }
    }

    pthread_mutex_unlock(&mutex_estado);
    printf("[Monitor] Monitoramento encerrado.\n");
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc == 2 && strcmp(argv[1], "sensor") == 0) {
        return executar_sensor();
    }

    struct mq_attr atributos = {0};
    atributos.mq_maxmsg = 10;
    atributos.mq_msgsize = sizeof(Mensagem);

    mq_unlink(FILA);
    mqd_t fila = mq_open(
        FILA, O_CREAT | O_RDWR, 0600, &atributos
    );

    if (fila == (mqd_t)-1) {
        perror("mq_open");
        return 1;
    }

    sem_init(&espacos_livres, 0, TAM_BUFFER);
    sem_init(&itens_disponiveis, 0, 0);

    pid_t processo_sensor = fork();

    if (processo_sensor == -1) {
        perror("fork");
        mq_close(fila);
        mq_unlink(FILA);
        return 1;
    }

    if (processo_sensor == 0) {
        execl("./estufa", "./estufa", "sensor", (char *)NULL);
        perror("exec");
        _exit(127);
    }

    pthread_t thread_recepcao;
    pthread_t thread_controle;
    pthread_t thread_monitor;

    pthread_create(&thread_monitor, NULL, monitorar_estado, NULL);
    pthread_create(&thread_controle, NULL, controlar_irrigacao, NULL);
    pthread_create(&thread_recepcao, NULL, receber_leituras, NULL);

    pthread_join(thread_recepcao, NULL);
    pthread_join(thread_controle, NULL);
    pthread_join(thread_monitor, NULL);

    int status;
    waitpid(processo_sensor, &status, 0);

    if (WIFEXITED(status)) {
        printf("[Principal] Processo do sensor finalizado com código %d.\n",
               WEXITSTATUS(status));
    }

    sem_destroy(&espacos_livres);
    sem_destroy(&itens_disponiveis);
    pthread_mutex_destroy(&mutex_buffer);
    pthread_mutex_destroy(&mutex_estado);
    pthread_cond_destroy(&cond_estado);

    mq_close(fila);
    mq_unlink(FILA);

    return 0;
}
