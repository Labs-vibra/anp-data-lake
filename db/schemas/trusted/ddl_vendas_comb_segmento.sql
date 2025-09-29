CREATE TABLE IF NOT EXISTS td_exp_anp.vendas_combustiveis_segmento (
    id INT64 OPTIONS (description="Identificador único do registro")
    data DATE OPTIONS (description="Data da Operação")
    unidade_da_federacao STRING OPTIONS(description="Unidade da Federação"),
    produto STRING OPTIONS(description="Nome do produto: Etanol, Óleo Diesel ou Gasolina C"),
    segmento STRING OPTIONS(description="Tipo de segmento, que pode ser: posto revendedor, TRR (Transportador-Revendedor-Retalhista) ou Consumidor Final"),
    vendas NUMERIC OPTIONS(description ="Volume vendido em m³"),
    data_criacao TIMESTAMP OPTIONS(description="Data de criação do registro na camada raw"),
    data_ingetao_td TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de injestão na camada trust")
)
PARTITION BY DATE(data_ingestao_td)
OPTIONS(
    description="Trata-se das vendas, pelas distribuidoras, de etanol, gasolina C e óleo diesel por segmento, ao longo de um período."
);