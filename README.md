# Olist Lakehouse

This project builds an analytics pipeline on Databricks using a Lakehouse architecture for Olist data. The goal is to collect, clean, transform, and present data for business analysis and dashboarding.

## Summary of Work Completed

- Collected data from 3 main sources:
  - PostgreSQL
  - Object Storage (CSV files stored in cloud storage)
  - API
- Used Databricks to build the ingestion and transformation workflow.
- Used Auto Loader and Lakeflow Declarative Pipelines to ingest data into the Bronze layer.
- Used Spark SQL / Apache Spark and Delta Lake to build the Silver and Gold layers.
- Built a semantic layer to define business metrics.
- Created a Databricks dashboard to visualize the results.
- Used Databricks Jobs to orchestrate the full workflow.
- Stored YAML/JSON workflow files in the databricks folder to document the pipeline structure when direct deployment is not possible on Databricks Free.

## Pipeline Architecture

1. Ingestion
   - Read data from PostgreSQL, object storage, and APIs.
   - Validate the initial schema.
   - Process raw data and load it into Bronze.

2. Transformation
   - Apply data normalization, cleaning, and quality checks.
   - Move data from Bronze to Silver.
   - Build dimensional and fact tables in the Gold layer.
   - Use Delta Lake to ensure consistency and query performance.

3. Semantic Layer
   - Create Metric Views.
   - Define business metrics such as revenue, orders, on-time delivery rate, and seller/product performance.

4. Dashboard
   - Create a dashboard in Databricks to track KPIs and support business analysis.

5. Orchestration
   - Use Databricks Jobs to run tasks in sequence: ingestion -> transformation -> analytics -> dashboard.

## Main Technologies

- Databricks
- Apache Spark
- Delta Lake
- Auto Loader
- Lakeflow Declarative Pipelines
- Spark SQL
- Databricks SQL / Dashboard
- Unity Catalog / catalog-schema structure

## Folder Structure

- pipelines/bronze_to_silver: transformation scripts from Bronze to Silver
- pipelines/silver_to_gold: scripts for building the Gold layer
- notebooks/ingestion: notebooks for ingestion and loading into Bronze
- notebooks/analytics: notebooks for metric views and analysis
- dashboards: dashboard assets
- databricks/pipelines: YAML files defining Databricks pipelines
- databricks/jobs: YAML files defining Databricks Jobs

## Role of Each Notebook

Each notebook in the notebooks/ingestion folder typically performs the following steps:

- Read data from the source
- Check schema
- Validate data quality
- Handle basic errors
- Write data to a Delta Table in Bronze

## Semantic Layer

Metric Views are created to provide a business-friendly layer for users and dashboards:

- Define business metrics
- Create metrics for sales, logistics, marketing, and inventory contexts
- Enable fast analysis without querying raw data directly

## Note on Databricks Free

Because the Databricks Free environment does not support full deployment, this repository stores YAML/JSON configuration files to represent the workflow and pipeline structure for documentation, version control, and import into the Databricks UI.

## Project Purpose

This project is not only a technical pipeline, but also an example of how to build a Lakehouse analytics workflow from raw data to business-ready dashboards in a way that is understandable and extendable.
