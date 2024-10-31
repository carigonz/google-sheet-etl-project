# Google Sheet ETL project

## Introduction

This project implements an ELT (Extract, Load, Transform) pipeline to retrieve data from an external Google Sheet and dump in an Amazon Readshift.

The usecase of this project is to generate a custom database from data provided by a supermarket. This supermarket has not access to any database, so we implemented a Google Form for generating an internal database en a GoogleSheet with data of merchandise seized because it has expired or is not suitable for sale, between other cases. This sheet, has a URL for a PDF file which has a table containing a detailed list of products with description, internal code and price. The goal of this project is to generate a process to find all PDFs notes, process them and dump them into a database, generating an own database for future supermarket metrics.

## Setup

You will need `docker`

1. Clone this repository:
   ```bash
   git clone https://github.com/carigonz/google-sheet-etl-project.git
   ```
2. Navigate into the project directory:
   ```bash
   cd google-sheet-etl-project
   ```
3. Configure your envs:
   - On root directory, copy `.env.example` and replace filename for `.env`
   - Paste your env values
   - On root directory copy your `credentials.json`
   - Make sure that env `CREDENTIALS_FILE` has the same value of your `credentials.json` filename.
4. Start the Airflow instance:
   ```bash
   docker-compose build
   docker-compose up -d
   ```
5. Access the Airflow user interface in your browser at [http://localhost:8080](http://localhost:8080):

   - **Username**: `airflow`
   - **Password**: `airflow`

6. After the above steps, search, unpause and trigger the DAG (`etl_redshift_load_dag`).

## Data Pipeline

### Load data from external API

Source data is a private Google Sheet database. Here is an example of data:
![image](https://github.com/user-attachments/assets/7ff20e30-ad4b-4ceb-98ef-977ef13ed158)

this is loaded in a dataframe, and in another one we have extracted table data from PDF files. Here is an exacmple of a file.
![image](https://github.com/user-attachments/assets/4aa38c33-058f-4603-aa8c-21ee442c468d)

> It is important to note that for some days, it could not be any notes to process.

### Transform data

We have three steps to purge this data:

- **map_custom_columns**: First we clean dataframe columns and map more friendly names.
- **remove_unused_columns**: Drop columns that are not in use from original form, or columns in PDF that we dont care about.
- **convert_data_types**: Some convertions for data types for more consistency.

### Load data

We use Readshift to generate this two tables, `devolutions` and `pdf_devolutions`

## Airflow DAGs

On this approach we have an unique dag.

We implemented some dummy tasks to avoid cases when there is no data for a particular date.

### GitHub Actions

- implemented for python linter flake8
- implemented for run tests
