# Crawler

Scripts para buscar dados do Ethereum e armazenar no seu banco dedados


## Getting Started

Crie um banco de dados PostgreSQL com o nome ethscan. 

Execute o conteúdo do arquivo database.sql nesse database.



### Prerequisites

PostgreSQL >= 9.5


Python 3.5


### Executando

main.py - Busca as atualizações da mempool.

monitor.py - Busca as transações da blockchain a partir de um bloco (Configurar no arquivo)

receiptCrawler.py - Busca os recibos das transações observadas na mempool que foram encontradas na blockchain

Sendo assim, executa-se obrigatóriamente uma etapa dependendo da outra.