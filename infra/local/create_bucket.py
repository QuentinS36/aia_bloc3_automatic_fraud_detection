"""Crée le bucket local (imitation S3) au démarrage de Docker Compose.

Ce script utilise boto3, la même librairie que le futur code d'ingestion,
pointée vers MinIO au lieu du vrai S3 grâce à `endpoint_url`. C'est ce
paramètre, et lui seul, qu'on changera pour basculer vers AWS plus tard.
"""
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

BUCKET = "fraud-detection"
ENDPOINT = "http://minio:9000"
MAX_TRIES = 20
WAIT_SECONDS = 3


def client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",  # valeur neutre, ignorée par MinIO
    )


def main():
    s3 = client()
    for attempt in range(1, MAX_TRIES + 1):
        try:
            existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
            if BUCKET in existing:
                print(f"Bucket '{BUCKET}' déjà présent.")
            else:
                s3.create_bucket(Bucket=BUCKET)
                print(f"Bucket '{BUCKET}' créé.")
            return
        except (ClientError, EndpointConnectionError) as e:
            print(f"[{attempt}/{MAX_TRIES}] MinIO pas encore prêt ({e}); nouvelle tentative dans {WAIT_SECONDS}s...")
            time.sleep(WAIT_SECONDS)
    print("Échec : MinIO n'a jamais répondu.", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
