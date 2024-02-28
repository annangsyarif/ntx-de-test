# Gunakan `python:3.11.4` sebagai base image
FROM python:3.11.4

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "etl.py"]