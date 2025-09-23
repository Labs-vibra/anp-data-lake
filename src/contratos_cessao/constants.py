# ETL de contratos de cessão de espaço e carregamento da ANP:
URL = (
    "https://www.gov.br/anp/pt-br/assuntos/distribuicao-e-revenda/distribuidor/distr/rd-ce-cf-qe/cessao-espaco-carregamento.xlsx"
)

TABLE_ID = "raw_ext_anp.contratos_cessao_espaco_carregamento"

MAPPING_COLUMNS = {
    'TIPO DE CONTRATO': 'tipo_contrato',
    'RAZÃO SOCIAL DA CEDENTE': 'razao_social_cedente',
    'CNPJ DA CEDENTE': 'cnpj_cedente',
    'N° AO DA CEDENTE': 'numero_ao_da_cedente',
    'MUNICÍPIO DA CEDENTE': 'municipio_cedente',
    'UF DA CEDENTE': 'uf_cedente',
    'RAZÃO SOCIAL DA CESSIONÁRIA': 'razao_social_cessionaria',
    'CNPJ DA CESSIONÁRIA': 'cnpj_cessionaria',
    'N°DAAEACESSIONÁRIA': 'numero_da_aea_cessionaria',
    'INÍCIO CONTRATO (ATO DE HOMOLOGAÇÃO)': 'inicio_contrato_ato_homologacao',
    'PROCESSO': 'processo',
    'TÉRMINO CONTRATO': 'termino_contrato',
    'VOLUME (m³)': 'volume_m3',
    'Gasolina A': 'gasolina_a',
    'Gasolina A - Premium': 'gasolina_a_premium',
    'Gasolina C': 'gasolina_c',
    'B100': 'b100',
    'EAC': 'eac',
    'EHC': 'ehc',
    'Óleo Diesel A S500': 'oleo_diesel_a_s500',
    'Óleo Diesel A S10': 'oleo_diesel_a_s10',
    'Óleo Diesel B S500': 'oleo_diesel_b_s500',
    'Óleo Diesel B S10': 'oleo_diesel_b_s10',
    'Diesel C S10': 'diesel_c_s10',
    'Óleo Diesel Mar': 'oleo_diesel_mar',
    'Óleo Combustível - 1A': 'oleo_combustivel_1a',
    'Óleo Combustível - 1B': 'oleo_combustivel_1b',
    'QAV': 'qav',
    'Querosene Iluminante': 'querosene_iluminante',
    'GAV': 'gav',
}