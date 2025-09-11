MERGE rf_ext_anp.logistica AS target
USING (
WITH
-- Tanto "vendas_atual" quanto "logistica_03" são consultas não utilizadas, talvez apagar posteriormente.
-- Elas foram mantidas aqui para referência futura, caso seja necessário.
-- Em "juncao_vendas_log03" que une ambas as consultas.
vendas_atual AS (
    SELECT
        data,
        agente_regulado AS razao_social_origem,
        codigo_produto AS cod_produto,
        nome_produto AS classe,
        descricao_produto AS dsc_produto,
        regiao_origem AS sigla_regiao_origem,
        uf_origem AS sigla_estado_origem,
        regiao_destinatario AS sigla_regiao_destino,
        uf_destino AS sigla_estado_destino,
        quantidade_produto_mil_m3 AS qtd_produto,
    FROM td_ext_anp.liquidos_vendas_atual
    WHERE 
        data_criacao = (SELECT MAX(data_criacao) FROM td_ext_anp.liquidos_vendas_atual)
),

logistica_03 AS (
    SELECT
        periodo AS data,
        produto,
        uf_origem AS sigla_estado_origem,
        uf_destino AS sigla_estado_destino,
        vendedor AS razao_social_origem,
        comprador AS razao_social_destino,
        qtd_produto_liquido / 1000000 AS qtd_produto,
    FROM td_ext_anp.logistica_03
    WHERE 
        data_criacao = (SELECT MAX(data_criacao) FROM td_ext_anp.logistica_03)
),

logistica_01 AS (
    SELECT
        modal
    FROM td_ext_anp.logistica_01
    WHERE 
        data_criacao = (SELECT MAX(data_criacao) FROM td_ext_anp.logistica_01)
),

instalacoes AS (
    SELECT
        cod_instalacao,
        num_cnpj_administrador_base AS cnpj_instalacao,
        nom_razao_social AS razao_social,
        nom_municipio,
        nom_estado,
    FROM td_ext_anp.codigos_instalacao
    WHERE 
        data_ingestao_td = (SELECT MAX(data_ingestao_td) FROM td_ext_anp.codigos_instalacao)
),

juncao_vendas_log03 AS (
    SELECT
        data,
        agente_regulado AS razao_social_origem,
        NULL AS razao_social_destino,
        codigo_produto AS cod_produto,
        nome_produto AS classe,
        descricao_produto AS dsc_produto,
        regiao_origem AS sigla_regiao_origem,
        uf_origem AS sigla_estado_origem,
        regiao_destinatario AS sigla_regiao_destino,
        uf_destino AS sigla_estado_destino,
        quantidade_produto_mil_m3 AS qtd_produto
    FROM td_ext_anp.liquidos_vendas_atual
    WHERE 
        data_criacao = (SELECT MAX(data_criacao) FROM td_ext_anp.liquidos_vendas_atual)

    UNION ALL

    SELECT
        periodo AS data,
        vendedor AS razao_social_origem,
        comprador AS razao_social_destino,
        NULL AS cod_produto,
        NULL AS classe,
        produto AS dsc_produto,
        NULL AS sigla_regiao_origem,
        uf_origem AS sigla_estado_origem,
        NULL AS sigla_regiao_destino,
        uf_destino AS sigla_estado_destino,
        qtd_produto_liquido / 1000000 AS qtd_produto
    FROM td_ext_anp.logistica_03
    WHERE 
        data_criacao = (SELECT MAX(data_criacao) FROM td_ext_anp.logistica_03)
),

sigla_estado_para_nome AS (
    SELECT
        *,
    CASE LOWER(sigla_estado_origem)
        WHEN 'ac' THEN 'acre'
        WHEN 'al' THEN 'alagoas'
        WHEN 'ap' THEN 'amapa'
        WHEN 'am' THEN 'amazonas'
        WHEN 'ba' THEN 'bahia'
        WHEN 'ce' THEN 'ceara'
        WHEN 'df' THEN 'distrito federal'
        WHEN 'es' THEN 'espirito santo'
        WHEN 'go' THEN 'goias'
        WHEN 'ma' THEN 'maranhao'
        WHEN 'mt' THEN 'mato grosso'
        WHEN 'ms' THEN 'mato grosso do sul'
        WHEN 'mg' THEN 'minas gerais'
        WHEN 'pa' THEN 'para'
        WHEN 'pb' THEN 'paraiba'
        WHEN 'pr' THEN 'parana'
        WHEN 'pe' THEN 'pernambuco'
        WHEN 'pi' THEN 'piaui'
        WHEN 'rj' THEN 'rio de janeiro'
        WHEN 'rn' THEN 'rio grande do norte'
        WHEN 'rs' THEN 'rio grande do sul'
        WHEN 'ro' THEN 'rondonia'
        WHEN 'rr' THEN 'roraima'
        WHEN 'sc' THEN 'santa catarina'
        WHEN 'sp' THEN 'sao paulo'
        WHEN 'se' THEN 'sergipe'
        WHEN 'to' THEN 'tocantins'
        ELSE NULL
    END AS nom_estado_origem,
    CASE LOWER(sigla_estado_destino)
        WHEN 'ac' THEN 'acre'
        WHEN 'al' THEN 'alagoas'
        WHEN 'ap' THEN 'amapa'
        WHEN 'am' THEN 'amazonas'
        WHEN 'ba' THEN 'bahia'
        WHEN 'ce' THEN 'ceara'
        WHEN 'df' THEN 'distrito federal'
        WHEN 'es' THEN 'espirito santo'
        WHEN 'go' THEN 'goias'
        WHEN 'ma' THEN 'maranhao'
        WHEN 'mt' THEN 'mato grosso'
        WHEN 'ms' THEN 'mato grosso do sul'
        WHEN 'mg' THEN 'minas gerais'
        WHEN 'pa' THEN 'para'
        WHEN 'pb' THEN 'paraiba'
        WHEN 'pr' THEN 'parana'
        WHEN 'pe' THEN 'pernambuco'
        WHEN 'pi' THEN 'piaui'
        WHEN 'rj' THEN 'rio de janeiro'
        WHEN 'rn' THEN 'rio grande do norte'
        WHEN 'rs' THEN 'rio grande do sul'
        WHEN 'ro' THEN 'rondonia'
        WHEN 'rr' THEN 'roraima'
        WHEN 'sc' THEN 'santa catarina'
        WHEN 'sp' THEN 'sao paulo'
        WHEN 'se' THEN 'sergipe'
        WHEN 'to' THEN 'tocantins'
        ELSE NULL
    END AS nom_estado_destino
    FROM juncao_vendas_log03
),

nome_estado_para_regiao AS (
    SELECT
        *,
    CASE
        WHEN nom_estado_origem IN ('acre', 'amapa', 'amazonas', 'para', 'rondonia', 'roraima', 'tocantins') THEN 'norte'
        WHEN nom_estado_origem IN ('alagoas', 'bahia', 'ceara', 'maranhao', 'paraiba', 'pernambuco', 'piaui', 'rio grande do norte', 'sergipe') THEN 'nordeste'
        WHEN nom_estado_origem IN ('distrito federal', 'goias', 'mato grosso', 'mato grosso do sul') THEN 'centro-oeste'
        WHEN nom_estado_origem IN ('espirito santo', 'minas gerais', 'rio de janeiro', 'sao paulo') THEN 'sudeste'
        WHEN nom_estado_origem IN ('parana', 'rio grande do sul', 'santa catarina') THEN 'sul'
        ELSE NULL
    END AS nom_regiao_origem,
    CASE
        WHEN nom_estado_destino IN ('acre', 'amapa', 'amazonas', 'para', 'rondonia', 'roraima', 'tocantins') THEN 'norte'
        WHEN nom_estado_destino IN ('alagoas', 'bahia', 'ceara', 'maranhao', 'paraiba', 'pernambuco', 'piaui', 'rio grande do norte', 'sergipe') THEN 'nordeste'
        WHEN nom_estado_destino IN ('distrito federal', 'goias', 'mato grosso', 'mato grosso do sul') THEN 'centro-oeste'
        WHEN nom_estado_destino IN ('espirito santo', 'minas gerais', 'rio de janeiro', 'sao paulo') THEN 'sudeste'
        WHEN nom_estado_destino IN ('parana', 'rio grande do sul', 'santa catarina') THEN 'sul'
        ELSE NULL
    END AS nom_regiao_destino
  FROM sigla_estado_para_nome
),

nome_regiao_para_sigla AS (
    SELECT
        data,
        LOWER(REGEXP_REPLACE(NORMALIZE(razao_social_origem, NFD), r'\pM', '')) AS razao_social_origem,
        LOWER(REGEXP_REPLACE(NORMALIZE(razao_social_destino, NFD), r'\pM', '')) AS razao_social_destino,
        cod_produto,
        classe,
        dsc_produto,
        sigla_estado_origem,
        sigla_estado_destino,
        qtd_produto,
        nom_estado_origem,
        nom_estado_destino,
        nom_regiao_origem,
        nom_regiao_destino,
    CASE LOWER(nom_regiao_origem)
        WHEN 'norte' THEN 'N'
        WHEN 'nordeste' THEN 'NE'
        WHEN 'centro-oeste' THEN 'CO'
        WHEN 'sudeste' THEN 'SE'
        WHEN 'sul' THEN 'S'
        ELSE NULL
    END AS sigla_regiao_origem,
    CASE LOWER(nom_regiao_destino)
        WHEN 'norte' THEN 'N'
        WHEN 'nordeste' THEN 'NE'
        WHEN 'centro-oeste' THEN 'CO'
        WHEN 'sudeste' THEN 'SE'
        WHEN 'sul' THEN 'S'
        ELSE NULL
    END AS sigla_regiao_destino
  FROM nome_estado_para_regiao
),

final_juncao_enriquecida AS (
    SELECT
        f.*,
        i_origem.cnpj_instalacao AS cnpj_instalacao_origem,
        i_origem.nom_municipio AS nom_municipio_origem,
        i_destino.cnpj_instalacao AS cnpj_instalacao_destino,
        i_destino.nom_municipio AS nom_municipio_destino
    FROM nome_regiao_para_sigla f
    LEFT JOIN instalacoes i_origem
        ON LOWER(f.razao_social_origem) = LOWER(i_origem.razao_social)
        AND LOWER(f.nom_estado_origem) = LOWER(i_origem.nom_estado)
    LEFT JOIN instalacoes i_destino
        ON LOWER(f.razao_social_destino) = LOWER(i_destino.razao_social)
        AND LOWER(f.nom_estado_destino) = LOWER(i_destino.nom_estado)
)

-- A Query até aqui está incompleta, esse select serve para ver como está o resultado parcial.
-- Remover as duas primeiras linhas para rodar corretamente.
SELECT * FROM final_juncao_enriquecida
