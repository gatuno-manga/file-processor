# Gatuno File Processor

Serviço de alta performance para processamento, sanitização e otimização de imagens utilizando Go e `libvips`.

## Variáveis de Ambiente

O serviço é configurado via variáveis de ambiente. Você pode encontrar um exemplo no arquivo `.env.example`.

| Variável | Descrição | Padrão |
|----------|-----------|---------|
| `APP_ENV` | Ambiente da aplicação (`development`, `production`). | `development` |
| `GRPC_PORT` | Porta para o servidor gRPC principal. | `50051` |
| `HEALTH_PORT` | Porta para endpoints de health check (`/healthz`, `/readyz`) e métricas Prometheus (`/metrics`). | `8081` |
| `WORKER_POOL_SIZE` | Tamanho do pool de workers concorrentes. Se for `0`, utiliza o número de CPUs disponíveis. | `0` |
| `KAFKA_BROKERS` | Lista de brokers Kafka separados por vírgula. | `localhost:9092` |
| `KAFKA_TOPIC_INPUT` | Tópico Kafka para receber requisições de processamento. | `image.processing.requested` |
| `KAFKA_TOPIC_OUTPUT` | Tópico Kafka para publicar eventos de conclusão. | `image.processing.completed` |
| `STORAGE_ENDPOINT` | Endpoint do storage compatível com S3 (RustFS, MinIO, AWS). | `localhost:9000` |
| `STORAGE_ACCESS_KEY` | Chave de acesso do S3. | - |
| `STORAGE_SECRET_KEY` | Chave secreta do S3. | - |
| `STORAGE_SSL` | Define se deve usar SSL/HTTPS para o storage. | `false` |
| `WEBP_QUALITY` | Qualidade da conversão para WebP (1-100). | `80` |
| `VIPS_MAX_CACHE` | Limite máximo de operações no cache do libvips. | `0` |
| `VIPS_MAX_CACHE_MEM` | Limite máximo de memória (em bytes) para o cache do libvips. | `0` |

## Como rodar

### Docker Compose

O arquivo `docker-compose.yml` contém apenas o serviço do processador. Ele assume que o Kafka e o Storage estão rodando externamente ou em outro container.

Para rodar o serviço:

```bash
docker compose up --build
```

Certifique-se de configurar corretamente os endereços em seu arquivo `.env` (ex: use o IP da sua máquina ou o nome da rede docker se estiverem na mesma rede).

*Nota: O mapeamento de portas padrão no `docker-compose.yml` foi ajustado para `50052` (gRPC) e `8082` (Health) para evitar conflitos locais.*

### Localmente

Certifique-se de ter as bibliotecas do `libvips` instaladas no seu sistema.

```bash
go run cmd/server/main.go
```
