<img width="1901" height="1033" alt="image" src="https://github.com/user-attachments/assets/cce6ac4d-ba0f-4f88-b330-2e56a344beb5" />
A ingestão de dados refere-se ao processo de coletar dados de diversas fontes e transferi-los para um local centralizado, onde serão armazenados, processados e analisados posteriormente.

Esse processo é fundamental para alimentar sistemas de análise, relatórios e aplicações de machine learning.

Objetivo: Coletar e reorganizar dados para inserção em um destino, preparando-os para processamento futuro ou uso imediato.

Importância: Uma ingestão mal planejada pode comprometer todas as etapas seguintes do pipeline de dados.



Aula da Nara Guimarães sobre Ingestão de Dados em Batch e Streaming foi a aula 13 do Bootcamp Data Girls, trilha Engenharia de Dados.



Tivemos como Desafio Proposto: Consumo e Transformação de Dados Kafka em DataFrame. Consegui colocar no VsCode a 



Sempre tive essa curiosidade sobre o Kafka e vi que ele foi originalmente desenvolvido no LinkedIn, é agora um projeto de código aberto da Apache Software Foundation. Ele foi projetado para lidar com grandes volumes de dados em tempo real, servindo como um sistema de mensageria de alta vazão e baixa latência.



**Cenário:**  

Você está desenvolvendo uma pipeline de ingestão e análise de dados em tempo real. Seu objetivo é consumir dados de um tópico Kafka específico, processar esses dados no consumidor e transformá-los em um DataFrame Pandas para posterior análise.



**Desafio**



1. **Produtor:**  

  Implemente um produtor Kafka que envie mensagens em formato JSON para um tópico chamado, por exemplo, `dados_usuarios`. Cada mensagem deve representar um registro de usuário com os seguintes campos:

  - `id` (int)

  - `nome` (str)

  - `idade` (int)

  - `cidade` (str)



2. **Consumidor:**  

  Implemente um consumidor Kafka que:

  - Consome as mensagens do tópico `dados_usuarios`.

  - Após receber um lote de N mensagens (por exemplo, N=5), transforme esse lote em um DataFrame Pandas.

  - Exiba o DataFrame no terminal e realize uma operação simples, como filtrar usuários acima de 30 anos ou agrupar por cidade.



3. **Requisitos Técnicos:**

  - Utilize Python e a biblioteca `confluent-kafka` ou `kafka-python`.

  - Utilize a biblioteca `pandas` para manipulação do DataFrame.

  - Comente bem o código para facilitar o entendimento.



**Dicas e Referências**



- O produtor pode gerar registros aleatórios ou ler de uma fonte externa (arquivo, API, etc.).

- O consumidor deve acumular as mensagens em uma lista, converter para DataFrame após atingir o tamanho do lote, e então processar.

- Veja exemplos de como consumir mensagens e transformar em DataFrame em [5].



**Exemplo de Fluxo Esperado**



```plaintext

Produtor envia:

{"id": 1, "nome": "Ana", "idade": 28, "cidade": "São Paulo"}

{"id": 2, "nome": "Carlos", "idade": 35, "cidade": "Rio de Janeiro"}

...



Consumidor recebe 5 mensagens, monta o DataFrame:

  id  nome idade     cidade

0  1   Ana   28   São Paulo

1  2 Carlos   35 Rio de Janeiro

...



Filtra usuários com idade > 30:

  id  nome idade     cidade

1  2 Carlos   35 Rio de Janeiro

...

```









Mais sobre kafka ...

Ingestão em Batch (Processamento em Lote)
Como funciona: O processamento em batch envolve a coleta e o processamento de grandes volumes de dados de uma só vez, em intervalos regulares. Os dados são acumulados por um período (horas, dias, semanas) e então processados em um único lote.
Exemplos de uso: Processamento de folhas de pagamento, análises financeiras diárias, cargas diárias em data warehouses.
Vantagens: Mais simples, barato e eficiente para tarefas que não exigem resposta imediata.
Desvantagens: Não é adequado para cenários que exigem atualização ou resposta em tempo real.
Ingestão em Streaming (Processamento em Tempo Real)
Como funciona: Os dados são processados quase instantaneamente, assim que chegam ao sistema, sem agrupamento prévio.
Exemplos de uso: Monitoramento de redes sociais, detecção de fraudes, sistemas de transações bancárias, análise de sentimentos em tempo real.
Vantagens: Permite respostas rápidas e decisões em tempo real.
Desvantagens: Exige infraestrutura mais robusta e pode ser mais caro, devido à necessidade de processamento contínuo e baixa latência.
Micro-Batching
Definição: Um modelo intermediário, onde pequenos lotes de dados são processados em intervalos curtos, combinando características de batch e streaming.
## **Apache Kafka**


Apache Kafka é uma plataforma de streaming distribuída de alta performance. Originalmente desenvolvido no LinkedIn, é agora um projeto de código aberto da Apache Software Foundation. Ele foi projetado para lidar com grandes volumes de dados em tempo real, servindo como um sistema de mensageria de alta vazão e baixa latência.


**Para que ele serve?**




Kafka serve principalmente para três propósitos:


- Publicar e assinar fluxos de registros (mensagens): Funciona como um sistema de mensagens de alto desempenho, permitindo que aplicações enviem e recebam eventos de forma assíncrona.
- Armazenar fluxos de registros de forma tolerante a falhas: Os dados são persistidos em disco e replicados, garantindo durabilidade e disponibilidade.
-Processar fluxos de registros à medida que eles ocorrem: Com as Kafka Streams API, é possível construir aplicações de processamento de fluxo diretamente no Kafka.


**Objetivos do Kafka:**


- Alto Throughput: Gerenciar milhões de mensagens por segundo.
- Baixa Latência: Entregar mensagens com atrasos mínimos.
- Durabilidade: Persistir mensagens de forma segura no disco.
- Tolerância a Falhas: Continuar operando mesmo com a falha de alguns nós.
- Escalabilidade: Crescer horizontalmente adicionando mais servidores.


**Arquitetura do Kafka: Conceitos Chave**
- Broker: Um servidor Kafka que armazena dados. Um cluster Kafka é composto por múltiplos brokers.
- Tópico (Topic): Uma categoria ou nome de feed para o qual os registros são publicados. Os dados em Kafka são organizados em tópicos.
- Partição (Partition): Um tópico é dividido em uma ou mais partições. Cada partição é uma sequência ordenada e imutável de registros. Isso permite a escalabilidade e paralelismo.
- Offset: Dentro de cada partição, cada registro tem um ID único e sequencial chamado offset.
- Produtor (Producer): Aplicações que publicam (escrevem) registros em tópicos Kafka.
- Consumidor (Consumer): Aplicações que assinam (lêem) registros de tópicos Kafka.
- Grupo de Consumidores (Consumer Group): Um conjunto de consumidores que trabalham juntos para ler dados de um ou mais tópicos. Cada partição é atribuída a apenas um consumidor dentro de um grupo, garantindo que as mensagens não sejam processadas duplicadamente dentro do mesmo grupo.
- Zookeeper: (Historicamente) Usado pelo Kafka para gerenciar os brokers, tópicos e partições, e para coordenação de cluster. Versões mais recentes do Kafka estão migrando para uma meta-arquitetura sem Zookeeper (KRaft).


**Casos de uso:**


Integração entre microsserviços, pipelines de dados em tempo real, monitoramento de logs, coleta de métricas, ingestão de dados para analytics.


