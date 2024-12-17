from google.cloud import bigquery
from datetime import date


def retrieve_data_from_bigquery(project_id, dataset_id, table_id, start_date=None, end_date=None):
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Construct the SQL query based on whether it's a range or single date
    if start_date and end_date:
        # Date range query
        query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE date BETWEEN @start_date AND @end_date
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date)
            ]
        )
    elif start_date and not end_date:
        # Single date query
        query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE date = @start_date
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date)
            ]
        )
    else:
        raise ValueError("Either start_date or both start_date and end_date must be provided.")

    # Execute the query
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    # Fetch and return results
    df = results.to_dataframe()

    return df

# Example usage
if __name__ == "__main__":
    import os

    PROJECT_ID = os.environ.get('PROJECT_ID')
    DATASET_ID = os.environ.get('DATASET_ID')
    TABLE_ID = os.environ.get('TABLE_ID')
    
    # For a single date
    single_date = date(2024, 11, 30)
    single_date_results = retrieve_data_from_bigquery(PROJECT_ID, DATASET_ID, TABLE_ID, start_date=single_date)
    print(f"Results for {single_date}:", single_date_results)

    # For a date range
    start_date = date(2024, 11, 14)
    end_date = date(2024, 11, 30)
    range_results = retrieve_data_from_bigquery(PROJECT_ID, DATASET_ID, TABLE_ID, start_date=start_date, end_date=end_date)
    range_results.to_csv(f'{start_date}_{end_date}.csv', index=False)
    print(f"Results for range {start_date} to {end_date}:", len(range_results))