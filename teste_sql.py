import pandasql as psql
from rw_ext_anp_cbios_2020 import rw_ext_anp_cbios_2020

test = rw_ext_anp_cbios_2020()

# Lê o arquivo SQL
with open("td_ext_anp_cbios_2020.sql", "r", encoding="utf-8") as f:
    query = f.read()

# Executa sobre o DataFrame df
result = psql.sqldf(query, locals())
print(result)
