
from google.cloud.exceptions import NotFound



def create_bq_dataset(project_id: str, datasets: list, location: str = "US"):
    from google.cloud import bigquery
    """Create bigquery dataset. Check first if the dataset exists
        Args:
            datasets_name: String    
    """
    client = bigquery.Client(project=project_id)
    
    for dataset_name in datasets:
        dataset_id = "{}.{}".format(client.project, dataset_name)
        try:
            client.get_dataset(dataset_id)
            print("Dataset {} already exists. Skipping creation.".format(dataset_id))
            return
        
        except NotFound:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = location
            # Make an API request
            dataset = client.create_dataset(dataset, timeout=30)
            print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
