# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
from datetime import datetime
import os
import json

import shared

from flask import Flask, request

from google.cloud import bigquery

app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    """
    Receives messages from a push subscription from Pub/Sub.
    Parses the message, and inserts it into BigQuery.
    """
    event = None
    envelope = request.get_json()

    # Check that data has been posted
    if not envelope:
        raise Exception("Expecting JSON payload")
    # Check that message is a valid pub/sub message
    if "message" not in envelope:
        raise Exception("Not a valid Pub/Sub Message")
    msg = envelope["message"]

    if "attributes" not in msg:
        raise Exception("Missing pubsub attributes")

    try:
        attr = msg["attributes"]

        # Header Event info
        if "headers" in attr:
            headers = json.loads(attr["headers"])

            # Process TeamCity Events
            if "X-Tcwebhooks-Request-Id" in headers:
                event = process_teamcity_event(msg)

        shared.insert_row_into_bigquery(event)

    except Exception as e:
        entry = {
                "severity": "WARNING",
                "msg": "Data not saved to BigQuery",
                "errors": str(e),
                "json_payload": envelope
            }
        print(json.dumps(entry))

    return "", 204


def process_teamcity_event(msg):
    # Unique hash for the event
    signature = shared.create_unique_id(msg)
    source = "teamcity"
    
    types = {"buildFinished"}

    redundant_keys = [
        "agentHostname",
        "agentName",
        "agentOs",
        "branch",
        "branchDisplayName",
        "branchIsDefault",
        "branchName",
        "buildExternalTypeId",
        "buildFullName",
        "buildInternalTypeId",
        "buildIsPersonal",
        "buildName",
        "buildResultDelta",
        "buildResultPrevious",
        "buildRunners",
        "buildStateDescription",
        "buildStatus",
        "buildStatusHtml",
        "buildStatusUrl",
        "buildTags",
        "buildTypeId",
        "changeFileListCount",
        "extraParameters",
        "derivedBuildEventType",
        "maxChangeFileListCountExceeded",
        "maxChangeFileListSize",
        "message",
        "rootUrl",
        "projectExternalId",
        "projectId",
        "projectInternalId",
        "teamcityProperties",
        "text",
        "triggeredBy",
    ]

    metadata = json.loads(base64.b64decode(msg["data"]).decode("utf-8").strip())["build"]
    #print(metadata)

    event_type = metadata["notifyType"]

    if event_type not in types:
        raise Exception("Unsupported TeamCity event: '%s'" % event_type)
    
    for key in redundant_keys:
        if key in metadata:
            metadata.pop(key, None)
    
    metadata["changes"] = [x["version"] for x in metadata["changes"] if x["change"]["vcsRoot"] != "deployments"]

    if event_type in ("buildFinished"):
        e_id = metadata["buildId"]
        event_type = "deployment"

    teamcity_event = {
        "event_type": event_type,
        "id": e_id,
        "metadata": json.dumps(metadata),
        "time_created": msg["publishTime"],
        "signature": signature,
        "msg_id": msg["message_id"],
        "source": source,
    }

    print(teamcity_event)

    return teamcity_event


if __name__ == "__main__":
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host="127.0.0.1", port=PORT, debug=True)
