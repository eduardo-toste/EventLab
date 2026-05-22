# EventLab

> Projeto em construção.

O **EventLab** é um projeto simples criado para estudar **Apache Kafka** e **MongoDB** com Java e Spring Boot.

A ideia é simular um fluxo básico de eventos:

Pedido criado  
→ evento `order.created`  
→ pagamento processado  
→ evento `payment.processed`  
→ notificação criada  
→ evento `notification.created`

## Objetivo

Este projeto tem como objetivo praticar:

- Kafka Producer
- Kafka Consumer
- Topics
- Consumer Groups
- Message Key
- EventId
- CorrelationId
- Retry
- Dead Letter Topic
- Idempotência
- MongoDB para logs de eventos

## Stack

- Java 21
- Spring Boot
- Spring Kafka
- MongoDB
- Docker Compose
- Kafka UI

## Status

Em construção.