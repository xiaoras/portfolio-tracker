FROM python:3.11-slim

ENV APP_NAME=portfolio-tracker
WORKDIR /usr/$APP_NAME

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY . .

EXPOSE 8080

CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]
