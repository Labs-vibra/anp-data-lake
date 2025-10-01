CREATE TABLE IF NOT EXISTS rw_ext_anp.vendas_combustiveis_segmento (
    ano STRING OPTIONS(description="Ano da venda"),
    mes STRING OPTIONS(description="Mês da venda"),  
    unidade_da_federacao STRING OPTIONS(description="Unidade da Federação"),
    produto STRING OPTIONS(description="Nome do produto: Etanol, Óleo Diesel ou Gasolina C"),
    segmento STRING OPTIONS(description="Tipo de segmento, que pode ser: posto revendedor, TRR (Transportador-Revendedor-Retalhista) ou Consumidor Final"),
    vendas STRING OPTIONS(description="Volume vendido em m³"),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de criação do registro na camada raw")
) 
PARTITION BY DATE(data_criacao)
OPTIONS(
    description="Trata-se das vendas, pelas distribuidoras, de etanol, gasolina C e óleo diesel por segmento, ao longo de um período."
);