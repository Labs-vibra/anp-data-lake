FROM python:3.9-slim

WORKDIR /app

COPY . .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

CMD ["python3", "rw_ext_anp_vendas_comb_segmento.py"]