# EventLab

O **EventLab** e um projeto de estudo para praticar **Apache Kafka** e **MongoDB** com **Java 21** e **Spring Boot**.

O fluxo atual do projeto simula uma cadeia simples de eventos:

`POST /orders` -> `order.created` -> `payment.processed` -> `notification.created`

O objetivo nao e construir um sistema de producao completo. O objetivo e aprender, de forma progressiva, os conceitos operacionais e de implementacao mais importantes de eventos assicronos.

## O que este projeto cobre

- Kafka Producer
- Kafka Consumer
- Topics
- Consumer Groups
- Message Key
- Partitions
- Ordering
- EventId
- CorrelationId
- MongoDB para event logs
- Idempotencia
- Retry
- Dead Letter Topic

## Stack

- Java 21
- Spring Boot
- Spring Kafka
- Spring Data MongoDB
- MongoDB
- Docker Compose
- Kafka UI

## Infra local

O projeto sobe os seguintes servicos:

- Kafka em `localhost:19092`
- Kafka UI em `http://localhost:8081`
- MongoDB em `localhost:27017`
- Aplicacao Spring em `http://localhost:8080`

Para iniciar a infraestrutura:

```bash
docker compose up -d
```

Para subir a aplicacao:

```bash
mvn spring-boot:run
```

## Fluxo atual

Hoje o projeto trabalha com estes topics:

- `order.created`
- `payment.processed`
- `notification.created`

O fluxo principal e:

1. o client faz `POST /orders`
2. a aplicacao cria o pedido e publica `order.created`
3. um consumer processa e publica `payment.processed`
4. outro consumer processa e publica `notification.created`

Isso cria um laboratorio pequeno, mas suficiente para estudar publicacao, consumo, encadeamento de eventos, key, partitions e rastreabilidade.

## Endpoint de teste

Para gerar um fluxo completo:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName":"Maria","total":120.0}'
```

Endpoint atual:

- `POST /orders`

Implementacao:

- [OrderController.java](/Users/eduardotoste/Documents/projects/EventLab/src/main/java/com/project/eventlab/controller/OrderController.java:1)

## Como estudar este repositorio

Uma ordem boa de estudo e:

1. subir Kafka, Kafka UI, MongoDB e a aplicacao
2. executar `POST /orders`
3. observar os topics no Kafka UI
4. ler as fases em ordem
5. implementar cada fase e validar no proprio ambiente

Se voce pular direto para retry, DLT ou idempotencia sem consolidar event chaining, key e consumer groups, o entendimento fica superficial.

## Status

Projeto em evolucao e orientado a estudo.

O codigo atual ja permite praticar o fluxo base e a documentacao cobre as fases de estudo de 4 a 10.
