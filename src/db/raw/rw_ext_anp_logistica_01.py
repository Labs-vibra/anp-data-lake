# df = df.rename(columns={
# 	"Período": "periodo",
# 	"Produto": "produto",
# 	"UF Origem": "uf_origem",
# 	"UF Destino": "uf_destino",
# 	"Vendedor": "vendedor",
# 	"Comprador": "comprador",
# 	"Qtd  Produto Líquido": "qtd_produto_liquido",
# })

# df['periodo'] = pd.to_datetime(df['periodo'], format='%Y/%m').dt.date
# df['qtd_produto_liquido'] = df['qtd_produto_liquido'].astype(float)
# df['data_criacao'] = pd.Timestamp.now(tz='America/Sao_Paulo')

# """
#     insert data into BigQuery with date-based partitioning
# """

# client = bigquery.Client()
# project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ext-ecole-biomassa")
# bq_dataset = "rw_ext_anp"
# table_name = "venda_congeneres"

# table_id = f"{project_id}.{bq_dataset}.{table_name}"

# job_config = bigquery.LoadJobConfig(
#     write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
# )


# partition_key = date.today().strftime('%Y%m%d')

# partitioned_table_id = f"{table_id}${partition_key}"
# print(f"Inserting data for partition: {partition_key}")

# try:
#     job = client.load_table_from_dataframe(
#         df, partitioned_table_id, job_config=job_config
#     )
#     job.result()
#     print(f"  Data for {partition_key} inserted successfully.")
# except Exception as e:
#     print(f"  Error inserting data for {partition_key}: {str(e)}")

# print("Data insertion completed!")
