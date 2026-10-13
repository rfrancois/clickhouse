FROM python:3

WORKDIR /app

ARG PROXY_HTTP, PROXY_HTTPS
RUN http_proxy="$PROXY_HTTP" https_proxy="$PROXY_HTTPS" pip install --verbose streamlit clickhouse-connect pandas tqdm scikit-learn numpy xxhash streamlit-aggrid pyvis

COPY . /app

CMD ["streamlit", "run", "main.py", "--server.address=0.0.0.0"]