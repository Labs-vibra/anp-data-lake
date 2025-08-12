select 
    razao_social,
    COALESCE(codigo_agente_regulado, 0) as codigo_agente_regulado,
    cnpj,
    COALESCE(somatorio_das_emissoes, 0) as somatorio_das_emissoes,
    participacao_de_mercado,
    COALESCE(meta_individual_2020, 0) as meta_individual_2020

from test

