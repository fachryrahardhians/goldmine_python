import os
import requests
import firebase_admin
import json
from firebase_admin import credentials, db, firestore
from datetime import datetime

import os

# Ambil isi key dari environment variable
firebase_key_json = os.environ.get("FIREBASE_KEY_JSON")

with open("serviceAccountKey.json", "w") as f:
    f.write(firebase_key_json)


# Firebase init
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)


def hit_api_and_save():

    with open("uri.json","r") as f:
        data = json.load(f)

    urls = data['uri']
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    for url in urls:
        main_jobs(url['title'],url['uri'],timestamp)


def main_jobs(title,url,timestamp):
    print(f"logging {title} data")
    # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        send_data(f"{title}",f"{title}_{timestamp}",data,timestamp)
    except Exception as e:
        error_log_to_firebase(e,timestamp)


def error_log_to_firebase(error,timestamp):
    db = firestore.client()
    doc_ref = db.collection("errors").document(f"err_{timestamp}")
    doc_ref.set({"message" : f"{error}"})
    print(f"ERROR ON {error}")


def send_data(collection,docs,data,timestamp):
    try:
        db = firestore.client()
        doc_ref = db.collection(collection).document(docs)
        doc_ref.set(data)
    except Exception as e:
        error_log_to_firebase(e,timestamp)

# Run once
hit_api_and_save()
