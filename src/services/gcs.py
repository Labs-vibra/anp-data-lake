import storage

def upload_file_to_gcs(local_file_path: str, bucket_name: str, dest_path: str):
    """
    Envia um arquivo local para o Cloud Storage.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_path)

    blob.upload_from_filename(local_file_path)
    print(f"✔️ Upload feito para: gs://{bucket_name}/{dest_path}")