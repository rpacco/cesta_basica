from google.cloud import bigquery
import logging


def save_to_bigquery(df, project_id, dataset_id, table_id, logger):
    """
    Append the given DataFrame to an existing BigQuery table if the data is newer.

    :param df: pandas DataFrame to be appended
    :param project_id: Google Cloud project ID
    :param dataset_id: BigQuery dataset ID
    :param table_id: Table ID within the dataset where data should be appended
    :return: dict with details or None if no data was appended
    """
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    
    # Check the last date in the table
    query = f"""
    SELECT MAX(date) AS last_date FROM `{project_id}.{dataset_id}.{table_id}`
    """
    query_job = client.query(query)
    results = query_job.result()
    last_date_in_table = next(results)['last_date']

    # Assuming 'date' column exists in df for comparison
    df_date = df['date'].max()
    
    if df_date > last_date_in_table:
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=[
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("price", "FLOAT"),
                bigquery.SchemaField("value", "FLOAT"),
                bigquery.SchemaField("unit_source", "STRING"),
                bigquery.SchemaField("category", "STRING"),
                bigquery.SchemaField("value_dieese", "FLOAT"),
                bigquery.SchemaField("adjusted_price", "FLOAT"),
                bigquery.SchemaField("date", "DATE"),
            ]
        )

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Waits for the job to complete.
        
        return {"rows_appended": job.output_rows, "date": str(df_date)}
    else:
        logger.log_text(f"The data date {df_date} is not newer than the last date in the table {last_date_in_table}. No data was uploaded.")
        return {"status": "no_update", "reason": "Data not newer", "last_date_in_table": str(last_date_in_table)}

if __name__ == "__main__":
    # Example usage:
    # df = your data frame
    # save_to_bigquery(df, 'your-project-id', 'your-dataset-id', 'your-table-id')
    pass