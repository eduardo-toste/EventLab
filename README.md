# EventLab

> Projeto em construção.

O **EventLab** é um laboratório prático criado para estudar **Kafka** e **MongoDB** com Java e Spring Boot.

A ideia do projeto é simular um fluxo simples de eventos, onde uma ação inicial publica uma mensagem no Kafka e outros componentes consomem essa mensagem para continuar o processamento.

Fluxo base do projeto:

Pedido fake criado  
→ evento `order.created` publicado  
→ pagamento fake processado  
→ evento `payment.processed` publicado  
→ notificação fake criada  
→ evento `notification.created` publicado  
→ eventos registrados no MongoDB

## Intuito do projeto

Este projeto está sendo desenvolvido com foco em estudo e prática de conceitos como:

- Apache Kafka
- Spring Kafka
- Producers e Consumers
- Topics
- Consumer Groups
- Message Key
- Event Envelope
- EventId e CorrelationId
- Event Chaining
- Retry
- Dead Letter Topic
- Idempotência
- MongoDB para logs e rastreabilidade de eventos

## Status

Em construção.