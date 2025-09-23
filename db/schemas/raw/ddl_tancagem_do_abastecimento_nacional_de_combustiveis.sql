CREATE TABLE IF NOT EXISTS rw_ext_anp.tancagem_do_abastecimento_nacional_de_combustiveis (
    data STRING OPTIONS(description="Data de referência da tancagem"),
    nome_empresarial STRING OPTIONS(description="Nome empresarial da empresa"),
    uf STRING OPTIONS(description="Unidade Federativa"),
    municipio STRING OPTIONS(description="Município"),
    cnpj STRING OPTIONS(description="CNPJ da empresa"),
    cod_instalacao STRING OPTIONS(description="Código da instalação"),
    segmento STRING OPTIONS(description="Segmento da instalação"),
    detalhe_instalacao STRING OPTIONS(description="Detalhe da instalação"),
    tag STRING OPTIONS(description="Tag da instalação"),
    tipo_da_unidade STRING OPTIONS(description="Tipo da unidade"),
    grupo_de_produtos STRING OPTIONS(description="Grupo de produtos"),
    tancagem_m3 STRING OPTIONS(description="Tancagem em metros cúbicos"),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de criação do registro na camada raw")
) PARTITION BY DATE(data_criacao);
