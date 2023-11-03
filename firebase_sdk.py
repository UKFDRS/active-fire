import json
import firebase_admin

import pandas as pd

from firebase_admin import db
from firebase_admin import credentials

cred = credentials.Certificate(
    "activefire-2e9a0-firebase-adminsdk-93vo8-d7087b5c37.json"
)
app = firebase_admin.initialize_app(
    cred,
    {
        "databaseURL": "https://activefire-2e9a0-default-rtdb.europe-west1.firebasedatabase.app"
    },
)

dfr = pd.read_parquet("firedata/data/active.parquet")
dfr.to_json("events_active.json", orient="records")
ref = db.reference("/events")
with open("events_active.json", "r") as f:
    file_contents = json.load(f)
ref.set(file_contents)
