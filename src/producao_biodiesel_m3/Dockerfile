FROM python:3.9-slim

WORKDIR /app

COPY . .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

CMD ["python3", "producao_biodiesel_m3_geral.py", "producao_biodiesel_m3_regiao"]
