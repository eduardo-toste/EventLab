# EventLab

O **EventLab** e um laboratorio de estudo para praticar **Apache Kafka**, **MongoDB**, **Java 21** e **Spring Boot** em um fluxo orientado a eventos.

O projeto implementa uma cadeia assincrona simples, mas com elementos operacionais importantes:

`POST /orders` -> `order.created` -> `payment.processed` -> `notification.created`

Hoje o laboratorio ja cobre:

- encadeamento de eventos
- `eventId` e `correlationId`
- `message key` com `orderId`
- consumer groups
- rastreabilidade em MongoDB
- idempotencia por `eventId + consumerName`
- retry com `DefaultErrorHandler`
- DLT por topico

## Stack

- Java 21
- Spring Boot 3.5
- Spring Web
- Spring Kafka
- Spring Data MongoDB
- MongoDB 7
- Apache Kafka 3.9
- Kafka UI
- Docker Compose
- Maven Wrapper

## Fluxo funcional

O fluxo principal funciona assim:

1. o cliente chama `POST /orders`
2. a aplicacao gera um `orderId` e publica `order.created`
3. `CreatedOrderConsumer` consome o evento e delega para `PaymentService`
4. `PaymentService` decide o status do pagamento e publica `payment.processed`
5. `PaymentProcessedConsumer` consome o evento e delega para `NotificationService`
6. `NotificationService` cria a notificacao e publica `notification.created`
7. os consumers de notificacao registram o consumo e permitem estudar `consumer groups`

## Regras atuais de negocio

- pagamentos com `total <= 1000.00` recebem status `APPROVED`
- pagamentos com `total > 1000.00` recebem status `FAILED`
- a notificacao publicada depende do status do pagamento
- todos os eventos da cadeia preservam o mesmo `correlationId`
- a key enviada para Kafka e sempre o `orderId`

## Topics

Topics principais:

- `order.created`
- `payment.processed`
- `notification.created`

Topics de dead letter:

- `order.created.dlt`
- `payment.processed.dlt`
- `notification.created.dlt`

## Consumers no projeto

Consumers de fluxo:

- `CreatedOrderConsumer`
- `PaymentProcessedConsumer`
- `NotificationCreatedConsumerA`

Consumers auxiliares para estudo de grupos:

- `NotificationCreatedConsumerB`
- `NotificationCreatedConsumerC`
- `NotificationCreatedConsumerD`

Consumer de DLT:

- `DeadLetterTopicConsumer`

Observacao importante sobre grupos:

- `NotificationCreatedConsumerA` e `NotificationCreatedConsumerB` compartilham o mesmo `groupId` logico de notificacao
- `NotificationCreatedConsumerC` e `NotificationCreatedConsumerD` usam grupos diferentes
- isso permite comparar distribuicao entre consumers do mesmo grupo e replicacao logica entre grupos distintos

## Persistencia em MongoDB

O projeto usa MongoDB para dois objetivos diferentes:

1. rastrear o fluxo dos eventos
2. proteger o processamento contra duplicacao

Colecoes atuais:

- `event_logs`: log de eventos publicados e consumidos
- `processed_events`: controle de idempotencia por `eventId + consumerName`
- `payments`: estado do pagamento e controle de publicacao
- `notifications`: estado da notificacao e controle de publicacao

## Infra local

Servicos expostos localmente:

- aplicacao Spring: `http://localhost:8080`
- Kafka broker externo: `localhost:19092`
- Kafka UI: `http://localhost:8081`
- MongoDB: `localhost:27017`

Credenciais MongoDB:

- usuario: `admin`
- senha: `admin`
- database: `eventlab`

## Como subir o ambiente

Subir Kafka, Kafka UI e MongoDB:

```bash
docker compose up -d
```

Subir a aplicacao:

```bash
./mvnw spring-boot:run
```

Rodar testes:

```bash
./mvnw test
```

## Endpoint disponivel

Endpoint atual:

- `POST /orders`

Exemplo de requisicao:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"customer-001","total":120.00}'
```

Resposta esperada:

```json
{
  "orderId": "generated-uuid",
  "status": "CREATED",
  "message": "Order created event published"
}
```

Implementacao:

- [OrderController.java](/Users/eduardotoste/Documents/projects/EventLab/src/main/java/com/project/eventlab/controller/OrderController.java:1)

## Retry e DLT

O projeto ja possui tratamento de falhas com Spring Kafka:

- `DefaultErrorHandler` com `FixedBackOff(1000ms, 2 tentativas)`
- `IllegalArgumentException` e `DuplicateKeyException` configuradas como excecoes sem retry
- envio automatico para DLT quando as tentativas acabam
- registro de consumo da DLT pelo `DeadLetterTopicConsumer`

Isso permite estudar:

- diferenca entre falha temporaria e falha permanente
- impacto de retry em processamento idempotente
- inspecao posterior de mensagens que sairam do fluxo principal

## Estado atual do laboratorio

O repositorio ja permite praticar, no codigo real, os conceitos das fases 4 a 10.

Ele nao tenta ser um sistema de producao completo. O foco continua sendo aprendizado progressivo, observabilidade do fluxo e entendimento operacional de processamento assincrono com Kafka.
