FROM apache/airflow:2.10.2-python3.11

# Instala Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Copia el archivo pyproject.toml y poetry.lock si está disponible
COPY pyproject.toml poetry.lock /opt/airflow/

# Instala las dependencias de Poetry
WORKDIR /opt/airflow
RUN poetry install --no-root

# Copia el resto de los archivos del proyecto
COPY . /opt/airflow/

ENV DB_HOST=${DB_HOST}
ENV DB_PORT=${DB_PORT}
ENV DB_NAME=${DB_NAME}
ENV DB_USER=${DB_USER}
ENV DB_PASSWORD=${DB_PASSWORD}

WORKDIR /opt/airflow

# Set the script as the entrypoint
# ENTRYPOINT ["/init_airflow.sh"]
