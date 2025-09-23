CREATE TABLE IF NOT EXISTS rw_ext_anp.tancagem_do_abastecimento_nacional_de_combustiveis (
    data STRING OPTIONS(description="Data de referência do levantamento da capacidade de tancagem."),
    nome_empresarial STRING OPTIONS(description="Razão social da empresa responsável pela instalação de armazenamento de combustíveis."),
    uf STRING OPTIONS(description="Unidade Federativa (estado) onde está localizada a instalação."),
    municipio STRING OPTIONS(description="Município onde está localizada a instalação."),
    cnpj STRING OPTIONS(description="CNPJ da empresa responsável pela instalação."),
    cod_instalacao STRING OPTIONS(description="Código único atribuído pela ANP para identificar a instalação de tancagem."),
    segmento STRING OPTIONS(description="Segmento de atuação da instalação (ex.: distribuição, refino, importação, etc.)."),
    detalhe_instalacao STRING OPTIONS(description="Descrição complementar do tipo de instalação (ex.: base primária, base secundária, terminal aquaviário, etc.)."),
    tag STRING OPTIONS(description="Código ou tag interna atribuída à instalação, usada para classificação e controle pela ANP."),
    tipo_da_unidade STRING OPTIONS(description="Categoria da unidade de armazenamento (ex.: terminal, base de distribuição, refinaria, etc.)."),
    grupo_de_produtos STRING OPTIONS(description="Categoria de produtos armazenados (ex.: gasolina, diesel, etanol, QAV)."),
    tancagem_m3 STRING OPTIONS(description="Capacidade total de armazenamento (tancagem) em metros cúbicos da instalação."),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de inserção do registro na camada raw.")
) PARTITION BY DATE(data_criacao);

