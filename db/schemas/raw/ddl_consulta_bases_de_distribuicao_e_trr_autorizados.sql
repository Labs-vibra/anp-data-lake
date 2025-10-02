CREATE TABLE IF NOT EXISTS rw_ext_anp.consulta_bases_de_distribuicao_e_trr_autorizados (
    cnpj STRING OPTIONS(description="Número de inscrição no CNPJ da empresa autorizada pela ANP. Identifica de forma única a pessoa jurídica responsável pela instalação."),
    razao_social STRING OPTIONS(description="Nome empresarial (razão social) da empresa autorizada."),
    numero_de_ordem STRING OPTIONS(description="Número sequencial atribuído pela ANP ao processo de autorização da instalação. Funciona como referência administrativa."),
    tipo_de_instalacao STRING OPTIONS(description="Tipo de instalação autorizada: **Base de Distribuição** (armazenamento e distribuição em grande escala) ou TRR – Transportador-Revendedor-Retalhista (comércio e transporte em menor escala)."),
    cep STRING OPTIONS(description="Código de Endereçamento Postal do endereço da matriz da empresa."),
    endereco_da_matriz STRING OPTIONS(description="Logradouro (rua, avenida, etc.) da matriz da empresa."),
    numero STRING OPTIONS(description="Número do endereço da matriz."),
    bairro STRING OPTIONS(description="Bairro onde a matriz da empresa está localizada."),
    complemento STRING OPTIONS(description="Complemento do endereço da matriz (ex.: sala, bloco, conjunto)."),
    capacidade_total STRING OPTIONS(description="Capacidade total de armazenagem da instalação (em m³), quando informada. Importante para medir a infraestrutura de distribuição ou revenda."),
    participacao_porcentagem STRING OPTIONS(description="Percentual de participação societária do acionista/empresa controladora na instalação, quando declarado."),
    administrador STRING OPTIONS(description="Nome do(s) administrador(es) ou responsável(is) legal(is) pela empresa/instalação."),
    numero_autorizacao STRING OPTIONS(description="Número formal da autorização emitida pela ANP que habilita a operação da instalação."),
    data_publicacao STRING OPTIONS(description="Data da publicação da autorização no Diário Oficial da União (DOU). Marca o início da validade oficial da autorização."),
    status_pmqc STRING OPTIONS(description="Situação da empresa no **Programa de Monitoramento da Qualidade de Combustíveis (PMQC)**, quando aplicável (ex.: participante, não participante, suspenso)."),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Data de criação do registro na camada raw")
)
PARTITION BY DATE(data_criacao)
OPTIONS(
    description="A fonte oferece acesso público a informações sobre as empresas autorizadas a operar como bases de distribuição e Transportadores Revendedores Retalhistas (TRR) no Brasil. Esses dados são essenciais para monitorar a cadeia de distribuição de combustíveis e garantir a conformidade regulatória no setor."
);
