#!/bin/bash
set -e

# Inicializar la base de datos de Airflow
airflow db init

# Verificar si ya existe un usuario administrador
if ! airflow users list | grep -q 'admin'; then
    # Crear un usuario administrador si no existe
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
    echo "Usuario administrador creado."
else
    echo "El usuario administrador ya existe."
fi

# Iniciar el webserver de Airflow
exec airflow webserver
