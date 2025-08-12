# Extração de dados da ANP - Logística

Consiste em 3 arquivos CSV compactados em um arquivo ZIP,
disponibilizados nos Dados Abertos do Painel de da ANP.

## Fluxo da extração

As funções de extração seguem as seguintes etapas:
1. Buscar a página web da ANP para localizar o link do arquivo ZIP atualizado.
2. Fazer o download do arquivo ZIP contendo os dados de logística.
3. Enviar o arquivo ZIP completo para um bucket no Google Cloud Storage.
4. Descompactar o arquivo ZIP em memória e envia individualmente os 3 arquivos CSV
   de logística para o bucket, para posterior processamento.

## Arquivos CSV incluídos

- **Logística 01:** Abastecimento nacional de combustíveis
- **Logística 02:** Vendas no mercado brasileiro de combustíveis
- **Logística 03:** Vendas congêneres de distribuidores
