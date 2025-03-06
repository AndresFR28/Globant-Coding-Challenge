FROM python:3.11.9

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && apt-get install -y git
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 5000

CMD ["python", "src/api.py"]
