# Consumer Thread Integration Guide

## Arquitetura Completa

```
┌─────────────┐
│ Producer 1  │──┐
├─────────────┤  │
│ Producer 2  │──┤
├─────────────┤  │     ┌──────────────┐       ┌──────────────┐
│ Producer 3  │──┼────→│   wt_queue   │──────→│ wt_consumer  │──→ wtree3
├─────────────┤  │     │   (MPSC)     │       │   (thread)   │     (LMDB)
│ Producer N  │──┘     └──────────────┘       └──────────────┘
└─────────────┘              ↓                        ↓
                       Zero-copy                Batch write
                       Ring buffer             + Monitoring
                       Lock-free tail          + Error handling
```

## Características Principais

### 1. **Zero Busy-Loop (0% CPU quando vazio)**
- Consumer thread usa `wtq_wait_nonempty()` que BLOQUEIA eficientemente
- Implementado com `wt_cond_wait()` → thread dorme até ser acordada
- Produtores acordam consumer via `wt_cond_signal()`

### 2. **Zero Deadlock**
- Swap é instantâneo (~nanossegundos)
- Processamento LMDB acontece FORA do lock da queue
- Produtores continuam enfilando enquanto consumer processa

### 3. **Monitoramento Completo**

#### Métricas Expostas:
```c
typedef struct {
    /* Throughput */
    uint64_t total_items_processed;
    uint64_t items_per_second;

    /* Latência */
    uint64_t avg_batch_latency_ms;
    uint64_t max_batch_latency_ms;
    uint64_t p95_batch_latency_ms;  // 95th percentile

    /* Health */
    uint64_t current_queue_depth;
    double queue_utilization;

    /* Erros */
    uint64_t total_errors;
    uint64_t consecutive_errors;
    uint64_t items_in_dlq;          // Dead letter queue

    /* Estado */
    bool is_running;
    bool is_healthy;
} wtc_metrics_t;
```

#### Como Usar:
```c
wtc_metrics_t metrics;
wtc_get_metrics(consumer, &metrics);

if (!wtc_is_healthy(consumer)) {
    // Alerta: consumer degradado
    // Verificar consecutive_errors, queue_depth, etc
}
```

### 4. **Tratamento de Erros Robusto**

#### Estratégias Disponíveis:

**a) FAIL_FAST** - Para no primeiro erro
```c
config.error_strategy = WTC_ERROR_FAIL_FAST;
```
- Útil para: debugging, testes, ambientes críticos

**b) RETRY** - Tenta novamente com backoff exponencial
```c
config.error_strategy = WTC_ERROR_RETRY;
config.max_retries = 3;
config.retry_backoff_ms = 100;  // 100ms, 200ms, 400ms
```
- Útil para: erros transientes (network, disco temporariamente cheio)

**c) DLQ (Dead Letter Queue)** - Move items falhados para fila especial
```c
config.error_strategy = WTC_ERROR_DLQ;

// Depois, inspecionar items falhados
size_t dlq_count;
void **dlq_items = wtc_get_dlq(consumer, &dlq_count);
// Processar manualmente, re-enfileirar, etc
```
- Útil para: análise post-mortem, retry manual, auditoria

**d) LOG_CONTINUE** - Loga erro e continua (PERDE DADOS)
```c
config.error_strategy = WTC_ERROR_LOG_CONTINUE;
```
- Útil para: dados não-críticos, telemetria best-effort

## Ciclo de Vida

### 1. Inicialização
```c
// Criar wtree3
wt3_t *wtree = wt3_open("db.mdb", 100*1024*1024, &err);

// Criar queue MPSC
wtq_t *queue = wtq_create(1000, free, NULL, free, NULL,
                          NULL, NULL, NULL, NULL);

// Configurar consumer
wtc_config_t config = wtc_default_config();
config.error_strategy = WTC_ERROR_RETRY;
config.log_fn = my_log_callback;

// Criar consumer
wt_consumer_t *consumer = wtc_create(queue, wtree, &config);

// Iniciar thread
wtc_start(consumer);
```

### 2. Operação Normal
```c
// Produtores enfileiram
wtq_enqueue(queue, key, key_len, value, val_len);

// Consumer automaticamente:
// 1. Espera em wtq_wait_nonempty() (CPU 0%)
// 2. Acorda quando tem items
// 3. Faz swap do buffer
// 4. Processa batch em transaction LMDB
// 5. Atualiza métricas
// 6. Volta para (1)
```

### 3. Monitoramento Contínuo
```c
// Thread separada (opcional)
while (running) {
    sleep(2);

    wtc_metrics_t m;
    wtc_get_metrics(consumer, &m);

    if (m.queue_depth > 900) {
        alert("Queue quase cheia!");
    }

    if (m.consecutive_errors > 5) {
        alert("Consumer com erros!");
    }

    if (m.avg_batch_latency_ms > 1000) {
        alert("Latência alta!");
    }
}
```

### 4. Shutdown Graceful
```c
// 1. Parar produtores (opcional, mas recomendado)
for (int i = 0; i < num_producers; i++) {
    producer_stop(producers[i]);
}

// 2. Esperar queue esvaziar
wtq_drain(queue);

// 3. Parar consumer
wtc_stop(consumer);  // Faz flush + join thread

// 4. Verificar métricas finais
wtc_metrics_t final;
wtc_get_metrics(consumer, &final);
printf("Processados: %llu items\n", final.total_items_processed);

// 5. Cleanup
wtc_destroy(consumer);
wtq_destroy(queue);
wt3_close(wtree);
```

## Cenários Avançados

### 1. Alta Carga (>10k items/sec)

**Problema**: Consumer não consegue acompanhar

**Soluções**:
```c
// a) Aumentar capacidade da queue
wtq_create(10000, ...);  // Mais buffer

// b) Processar em paralelo (worker pool)
// Consumer faz swap → distribui para workers
// Workers escrevem em parallel (txns separadas)

// c) Sharding (múltiplas queues)
// hash(key) % N_QUEUES → escolhe queue
// N consumers independentes, um por queue
```

### 2. Backpressure (Produtores muito rápidos)

**Problema**: Queue enche, produtores bloqueiam

**Solução 1 - Callback on_full**:
```c
void on_full_callback(void *arg) {
    // Opções:
    // 1. Log warning
    // 2. Incrementar contador de backpressure
    // 3. Notificar produtores para diminuir taxa
}

wtq_create(1000, ..., on_full_callback, NULL, ...);
```

**Solução 2 - Fila dinâmica**:
```c
// Quando depth > 80%:
wtq_swap_buffer(q, capacity * 2);  // Dobra capacidade
```

### 3. Priorização

**Problema**: Items urgentes devem ser processados primeiro

**Solução - Múltiplas Queues**:
```c
wtq_t *high_prio_queue = wtq_create(...);
wtq_t *low_prio_queue = wtq_create(...);

// Consumer loop:
while (true) {
    // Sempre processa high priority primeiro
    if (wtq_depth(high_prio_queue) > 0) {
        process_queue(high_prio_queue);
    } else {
        process_queue(low_prio_queue);
    }
}
```

### 4. Crash Recovery

**Problema**: Consumer crashou, items no buffer swapped foram perdidos

**Solução - Persistência antes de commit**:
```c
// No process_batch():
wtq_buffer_t buf = wtq_swap_buffer(q, 0);

// 1. Serializar buffer para disco
save_buffer_to_disk("buffer_pending.dat", &buf);

// 2. Processar
process_items(buf);

// 3. Deletar arquivo
unlink("buffer_pending.dat");

// No restart:
if (file_exists("buffer_pending.dat")) {
    // Recarregar e processar buffer pendente
    buf = load_buffer_from_disk("buffer_pending.dat");
    process_items(buf);
}
```

## Performance Tips

### 1. Tamanho Ótimo do Batch
```c
// Regra geral: 1-5 segundos de carga típica
// Exemplo: 1000 items/sec → batch de 1000-5000

// Muito pequeno: overhead de transaction commits
// Muito grande: latência alta
```

### 2. Latência vs Throughput
```c
// Baixa latência (web request):
config.commit_interval_ms = 100;  // Commit a cada 100ms

// Alto throughput (analytics):
config.commit_interval_ms = 5000; // Batch grande
```

### 3. Múltiplos Consumers
```c
// NUNCA: múltiplos consumers na MESMA queue (MPSC, não MPMC)

// SIM: Sharding
for (int i = 0; i < N; i++) {
    queues[i] = wtq_create(...);
    consumers[i] = wtc_create(queues[i], wtrees[i], ...);
    wtc_start(consumers[i]);
}

// Produtor escolhe queue:
int queue_idx = hash(key) % N;
wtq_enqueue(queues[queue_idx], ...);
```

## Troubleshooting

### Queue sempre vazia (depth = 0)
- **Causa**: Produtores não estão enfileirando
- **Verificar**: `wtq_enqueue()` retorna true?
- **Verificar**: Queue foi flushed acidentalmente?

### Queue sempre cheia (depth = capacity)
- **Causa**: Consumer lento ou travado
- **Verificar**: `wtc_is_healthy()` retorna true?
- **Verificar**: Latência alta? Erros?
- **Ação**: Ver logs, verificar LMDB disk I/O

### Consecutive errors aumentando
- **Causa**: LMDB com problemas (disk full, corruption, permissions)
- **Verificar**: Logs de erro
- **Ação**: Verificar espaço em disco, permissões, integridade DB

### Latência P95 muito alta
- **Causa**: Batches muito grandes, ou LMDB lento
- **Ação**: Reduzir `max_batch_size`, otimizar LMDB (SSD, tuning)

## Compilação

```bash
# Adicionar ao CMakeLists.txt:
add_library(wt_consumer STATIC
    src/wt_consumer.c
)

target_link_libraries(wt_consumer PUBLIC
    wt_queue
    wtree3
    wt_sync
)

# Compilar exemplo:
gcc -o consumer_example examples/consumer_example.c \
    -lwt_consumer -lwt_queue -lwtree3 -lwt_sync -llmdb -lpthread
```

## Conclusão

Esta implementação oferece:

✅ **Zero busy-loop** - CPU 0% quando idle
✅ **Zero deadlock** - Processamento offline
✅ **Monitoramento completo** - Métricas detalhadas
✅ **Error handling robusto** - 4 estratégias
✅ **Graceful shutdown** - Sem perda de dados
✅ **Production-ready** - Testado e robusto

**Próximos passos**: Compile o exemplo, rode, observe as métricas! 🚀
