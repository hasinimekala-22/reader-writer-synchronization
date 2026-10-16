
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>

#define NUM_READERS 4
#define NUM_WRITERS 2

int shared_resource = 100;
int read_count = 0;

pthread_mutex_t mutex;
pthread_mutex_t print_mutex;
sem_t rw_mutex;

/* Reader function */
void *reader(void *arg)
{
    int id = *(int *)arg;

    /* Entry section */
    pthread_mutex_lock(&mutex);

    read_count++;

    if (read_count == 1)
        sem_wait(&rw_mutex);

    pthread_mutex_unlock(&mutex);

    /* Reading section */
    pthread_mutex_lock(&print_mutex);
    printf("[READER %d] Reading value: %d\n",
           id, shared_resource);
    printf("[READER %d] Active readers: %d\n",
           id, read_count);
    pthread_mutex_unlock(&print_mutex);

    sleep(2);

    /* Exit section */
    pthread_mutex_lock(&mutex);

    read_count--;

    pthread_mutex_lock(&print_mutex);
    printf("[READER %d] Finished reading.\n", id);
    pthread_mutex_unlock(&print_mutex);

    if (read_count == 0)
        sem_post(&rw_mutex);

    pthread_mutex_unlock(&mutex);

    return NULL;
}

/* Writer function */
void *writer(void *arg)
{
    int id = *(int *)arg;

    pthread_mutex_lock(&print_mutex);
    printf("[WRITER %d] Waiting for access...\n", id);
    pthread_mutex_unlock(&print_mutex);

    /* Request exclusive access */
    sem_wait(&rw_mutex);

    /* Writing section */
    pthread_mutex_lock(&print_mutex);

    printf("[WRITER %d] Writing...\n", id);

    shared_resource += 10;

    printf("[WRITER %d] Updated value: %d\n",
           id, shared_resource);

    pthread_mutex_unlock(&print_mutex);

    sleep(2);

    pthread_mutex_lock(&print_mutex);
    printf("[WRITER %d] Finished writing.\n", id);
    pthread_mutex_unlock(&print_mutex);

    /* Release exclusive access */
    sem_post(&rw_mutex);

    return NULL;
}

int main(void)
{
    pthread_t readers[NUM_READERS];
    pthread_t writers[NUM_WRITERS];

    int reader_ids[NUM_READERS];
    int writer_ids[NUM_WRITERS];

    pthread_mutex_init(&mutex, NULL);
    pthread_mutex_init(&print_mutex, NULL);
    sem_init(&rw_mutex, 0, 1);

    printf("====================================\n");
    printf(" READER-WRITER SYNCHRONIZATION\n");
    printf("====================================\n");
    printf("Initial shared resource: %d\n",
           shared_resource);
    printf("Readers: %d | Writers: %d\n",
           NUM_READERS, NUM_WRITERS);
    printf("------------------------------------\n\n");

    /* Create reader threads */
    for (int i = 0; i < NUM_READERS; i++) {
        reader_ids[i] = i + 1;

        pthread_create(&readers[i], NULL,
                       reader, &reader_ids[i]);
    }

    /* Create writer threads */
    for (int i = 0; i < NUM_WRITERS; i++) {
        writer_ids[i] = i + 1;

        pthread_create(&writers[i], NULL,
                       writer, &writer_ids[i]);
    }

    /* Wait for all threads */
    for (int i = 0; i < NUM_READERS; i++)
        pthread_join(readers[i], NULL);

    for (int i = 0; i < NUM_WRITERS; i++)
        pthread_join(writers[i], NULL);

    printf("\n====================================\n");
    printf(" SIMULATION COMPLETED\n");
    printf("====================================\n");
    printf("Initial value : 100\n");
    printf("Final value   : %d\n", shared_resource);
    printf("Expected value: 120\n");
    printf("====================================\n");

    sem_destroy(&rw_mutex);
    pthread_mutex_destroy(&mutex);
    pthread_mutex_destroy(&print_mutex);

    return 0;
}
