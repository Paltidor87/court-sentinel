import os
import firebase_admin
from firebase_admin import credentials, firestore

def init_firebase():
    """Initialize Firebase using Application Default Credentials (GCP/Hackathon credits)."""
    if not firebase_admin._apps:
        # Use ADC (Application Default Credentials)
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, {
            'projectId': os.getenv("GOOGLE_CLOUD_PROJECT", "gcloud-hackathon-hauvzosacm3d0"),
        })
    return firestore.client()

db = init_firebase()
