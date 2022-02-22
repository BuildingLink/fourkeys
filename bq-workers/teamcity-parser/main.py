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
                changes = [x["version"] for x in event["changes"]]
                map_changes_to_project(changes, )

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
        "branch",
        "buildResultDelta",
        "buildResultPrevious",
        "buildStatus",
        "buildRunners",
        "extraParameters",
        "teamcityProperties",
    ]

    metadata = json.loads(base64.b64decode(msg["data"]).decode("utf-8").strip())["build"]
    print(metadata)

    event_type = metadata["notifyType"]

    if event_type not in types:
        raise Exception("Unsupported TeamCity event: '%s'" % event_type)
    
    for key in redundant_keys:
        if key in metadata:
            metadata.pop(key, None)
    
    metadata["changes"] = [x for x in metadata["changes"] if x["change"]["vcsRoot"] != "deployments"]

    if event_type in ("buildFinished"):
        e_id = metadata["buildId"]
        event_type = "deployment"

    teamcity_event = {
        "event_type": event_type,
        "id": e_id,
        "project": metadata["projectName"],
        "metadata": json.dumps(metadata),
        "time_created": msg["publishTime"],
        "signature": signature,
        "msg_id": msg["message_id"],
        "source": source,
    }

    print(teamcity_event)

    return teamcity_event

def map_changes_to_project(change_ids, project):
    if (not change_ids) or (not project):
        raise Exception("No data to update")

    print(f"{len(change_ids)} changes to process")

    # Set up bigquery instance
    client = bigquery.Client()
    dataset_id = "four_keys"
    table_id = "events_raw"

    num_dml_affected_rows = 0

    for change_id in change_ids:
        query_text = f"UPDATE `{dataset_id}.{table_id}` SET project = {project} WHERE change = {change_id}"

        query_job = client.query(query_text)

        # Wait for query job to finish.
        query_job.result()

        if query_job.num_dml_affected_rows > 0:
            print(f"{change_id} change was mapped to {project} project")
        
        num_dml_affected_rows += query_job.num_dml_affected_rows

    print(f"DML query modified {num_dml_affected_rows} rows.")
    return query_job.num_dml_affected_rows

event = {
    'build': {
        'buildStatus': 'Success',
        'buildResult': 'success',
        'buildResultPrevious': 'success',
        'buildResultDelta': 'unchanged',
        'notifyType': 'buildFinished',
        'buildFullName': 'ShiftLog / Deploy to Dev',
        'buildName': 'Deploy to Dev',
        'buildId': '36599',
        'buildTypeId': 'ShiftLog_DeployToDev',
        'buildInternalTypeId': 'bt628',
        'buildExternalTypeId': 'ShiftLog_DeployToDev',
        'buildStatusUrl': 'https://teamcity.buildinglink.com/viewLog.html?buildTypeId=ShiftLog_DeployToDev&buildId=36599',
        'buildStatusHtml': '<span class="tcWebHooksMessage"><a href="https://teamcity.buildinglink.com/project.html?projectId=ShiftLog">ShiftLog</a> :: <a href="https://teamcity.buildinglink.com/viewType.html?buildTypeId=ShiftLog_DeployToDev">Deploy to Dev</a> # <a href="https://teamcity.buildinglink.com/viewLog.html?buildTypeId=ShiftLog_DeployToDev&buildId=36599"><strong>1.0.45</strong></a> has <strong>finished</strong> with a status of <a href="https://teamcity.buildinglink.com/viewLog.html?buildTypeId=ShiftLog_DeployToDev&buildId=36599"> <strong>success</strong></a> and was triggered by <strong>you</strong></span>',
        'buildStartTime': '',
        'currentTime': '',
        'rootUrl': 'https://teamcity.buildinglink.com',
        'projectName': 'ShiftLog',
        'projectId': 'ShiftLog',
        'projectInternalId': 'project60',
        'projectExternalId': 'ShiftLog',
        'buildNumber': '1.0.45',
        'agentName': 'BLKPDC75-1',
        'agentOs': 'Linux, version 5.4.0-99-generic',
        'agentHostname': '10.1.40.75',
        'triggeredBy': 'you',
        'message': 'Build ShiftLog / Deploy to Dev has finished. This is build number 1.0.45, has a status of "success" and was triggered by you',
        'text': 'ShiftLog / Deploy to Dev has finished. Status: success',
        'branchName': '<default>',
        'branchDisplayName': 'master',
        'buildStateDescription': 'finished',
        'branchIsDefault': True,
        'buildIsPersonal': False,
        'derivedBuildEventType': 'BUILD_SUCCESSFUL',
        'branch': {'@class': 'webhook.teamcity.payload.content.WebHooksBranchImpl', 'displayName': 'master', 'name': '<default>', 'isDefaultBranch': True},
        'buildRunners': ['Command Line', 'Command Line', 'Command Line', 'Command Line', 'PowerShell', 'PowerShell'],
        'buildTags': [],
        'extraParameters': [{'name': 'preferredDateFormat', 'value': ''}],
        'teamcityProperties': [],
        'maxChangeFileListSize': 100,
        'maxChangeFileListCountExceeded': False,
        'changeFileListCount': 96,
        'changes': [
            {'version': 'c5c68c4c7a97f175f74e91fdfd82ef102fb35d80', 'change': {'files': ['estates/dev/bms/amenities/deployment-api.yaml', 'estates/dev/bms/amenities/deployment-email-consumer.yaml'], 'comment': 'Updating image\n', 'username': 'teamcity', 'vcsRoot': 'deployments'}},
            {'version': '08da196ca56579441d612eaa661dceee98a54e18', 'change': {'files': ['estates/dev/bms/amenities/deployment-api.yaml', 'estates/dev/bms/amenities/deployment-email-consumer.yaml'], 'comment': 'Updating image\n', 'username': 'teamcity', 'vcsRoot': 'deployments'}},
            {'version': 'c41b26082f86eb09ec03db89835e6ea9e5fc782e', 'change': {'files': ['estates/dev/bms/contentcreator/deployment-announcements-sync.yaml', 'estates/dev/bms/contentcreator/deployment-api.yaml', 'estates/dev/bms/contentcreator/deployment-email-send-queue.yaml'], 'comment': 'Updating image\n', 'username': 'teamcity', 'vcsRoot': 'deployments'}},
            {'version': '03ed1fb03e95abf5e4b98788c72bf512dd84fea4', 'change': {'files': ['estates/dev/bms/shiftlog/deployment-api.yaml'], 'comment': 'Updating image\n', 'username': 'teamcity', 'vcsRoot': 'deployments'}},
            {'version': '76440f9038c608ad0b8e320c00dd85582ed1c205', 'change': {'files': [], 'comment': 'Merge pull request #21 from BuildingLink/cap-535-stylecop\n\nAdd StyleCop Analyzer package into 2 projects', 'username': 'oleg', 'vcsRoot': 'ShiftLog'}},
            {'version': 'a6b5aed808a17f8d95d26203cb32f8da26bcdb91', 'change': {'files': ['BuildingLink.ShiftLog.Tests/Controllers/CustomWebApplicationFactory.cs', 'BuildingLink.ShiftLog.Tests/Controllers/NoteControllerTests.cs', 'BuildingLink.ShiftLog.Tests/Controllers/Security/RoleIntegrationTests.cs', 'BuildingLink.ShiftLog.Tests/Mapping/EmployeeBlobConverterTests.cs', 'BuildingLink.ShiftLog.Tests/Mapping/ShiftlogMappingTests.cs', 'BuildingLink.ShiftLog.Tests/Repositories/CustomWebApplicationFactory.cs', 'BuildingLink.ShiftLog.Tests/Repositories/NotesRepositoryTests.cs', 'BuildingLink.ShiftLog.Tests/Repositories/UnitRepositoryTests.cs', 'BuildingLink.ShiftLog.Tests/Services/PropertyEmployee/NotesServiceTests.cs', 'BuildingLink.ShiftLog.Tests/Services/PropertyEmployee/UnitServiceTests.cs', 'BuildingLink.ShiftLog/Authentication/Roles.cs', 'BuildingLink.ShiftLog/BuildingLink.ShiftLog.xml', 'BuildingLink.ShiftLog/Configuration/ServiceCollectionExtensions.cs', 'BuildingLink.ShiftLog/Controllers/NoteController.cs', 'BuildingLink.ShiftLog/Controllers/UnitController.cs', 'BuildingLink.ShiftLog/Data/V2DatabaseFactory.cs', 'BuildingLink.ShiftLog/HostExtensions.cs', 'BuildingLink.ShiftLog/Logging/IdentityEnricher.cs', 'BuildingLink.ShiftLog/Mapping/EmployeeBlobConverter.cs', 'BuildingLink.ShiftLog/Mapping/ShiftlogMapping.cs', 'BuildingLink.ShiftLog/Program.cs', 'BuildingLink.ShiftLog/Repositories/DataSeeding.cs', 'BuildingLink.ShiftLog/Repositories/NoteRepository.cs', 'BuildingLink.ShiftLog/Repositories/UnitRepository.cs', 'BuildingLink.ShiftLog/Services/ShiftLogLocalDbSeedingService.cs', 'BuildingLink.ShiftLog/Startup.cs'], 'comment': 'fixed warnings after style cop analyzer\n', 'username': 'oleg', 'vcsRoot': 'ShiftLog'}},
            {'version': 'd4fe2050472ba36706fb3253a748cf0beb650f10', 'change': {'files': ['estates/dev/bms/contentcreator/deployment-announcements-sync.yaml', 'estates/dev/bms/contentcreator/deployment-api.yaml', 'estates/dev/bms/contentcreator/deployment-email-send-queue.yaml'], 'comment': 'Updating image\n', 'username': 'teamcity', 'vcsRoot': 'deployments'}},
            {'version': 'fe907f42a9f3fcadd5c0e79296d28856efb195bb', 'change': {'files': ['.editorconfig', 'BuildingLink.ShiftLog.Tests/Authentication/IdentityDefaults.cs', 'BuildingLink.ShiftLog.Tests/Controllers/CustomWebApplicationFactory.cs', 'BuildingLink.ShiftLog.Tests/Controllers/NoteControllerTests.cs', 'BuildingLink.ShiftLog.Tests/Controllers/Security/RoleCreatorIntegrationTests.cs', 'BuildingLink.ShiftLog.Tests/Controllers/Security/RoleIntegrationTests.cs', 'BuildingLink.ShiftLog.Tests/Controllers/Security/RoleReaderIntegrationTests.cs', 'BuildingLink.ShiftLog.Tests/Controllers/UnitControllerTests.cs', 'BuildingLink.ShiftLog.Tests/Extensions/ServiceCollectionExtensions.cs', 'BuildingLink.ShiftLog.Tests/Mapping/EmployeeBlobConverterTests.cs', 'BuildingLink.ShiftLog.Tests/Mapping/ShiftlogMappingTests.cs', 'BuildingLink.ShiftLog.Tests/Repositories/CustomWebApplicationFactory.cs', 'BuildingLink.ShiftLog.Tests/Repositories/NotesRepositoryTests.cs', 'BuildingLink.ShiftLog.Tests/Repositories/UnitRepositoryTests.cs', 'BuildingLink.ShiftLog.Tests/Services/PropertyEmployee/NotesServiceTests.cs', 'BuildingLink.ShiftLog.Tests/Services/PropertyEmployee/UnitServiceTests.cs', 'BuildingLink.ShiftLog/Builders/NoteBuilder.cs', 'BuildingLink.ShiftLog/Builders/UnitBuilder.cs', 'BuildingLink.ShiftLog/Builders/UserBuilder.cs', 'BuildingLink.ShiftLog/BuildingLink.ShiftLog.xml', 'BuildingLink.ShiftLog/Configuration/ServiceCollectionExtensions.cs', 'BuildingLink.ShiftLog/Controllers/NoteController.cs', 'BuildingLink.ShiftLog/Controllers/UnitController.cs', 'BuildingLink.ShiftLog/Data/V2DatabaseFactory.cs', 'BuildingLink.ShiftLog/HostExtensions.cs', 'BuildingLink.ShiftLog/Logging/IdentityEnricher.cs', 'BuildingLink.ShiftLog/Mapping/ShiftlogMapping.cs', 'BuildingLink.ShiftLog/Models/EmployeeBlob.cs', 'BuildingLink.ShiftLog/Program.cs', 'BuildingLink.ShiftLog/Repositories/DataSeeding.cs', 'BuildingLink.ShiftLog/Repositories/INotesRepository.cs', 'BuildingLink.ShiftLog/Repositories/IUnitRepository.cs', 'BuildingLink.ShiftLog/Repositories/NoteRepository.cs', 'BuildingLink.ShiftLog/Repositories/UnitRepository.cs', 'BuildingLink.ShiftLog/Services/PropertyEmployee/IUnitService.cs', 'BuildingLink.ShiftLog/Services/PropertyEmployee/NoteService.cs', 'BuildingLink.ShiftLog/Services/ShiftLogLocalDbSeedingService.cs', 'BuildingLink.ShiftLog/Startup.cs', 'ShiftLog.sln'], 'comment': "added style cop 'editorconfig' configuraton file\nfixed errors after code analyzer\n", 'username': 'oleg', 'vcsRoot': 'ShiftLog'}},
            {'version': '4e381c2b1bcacd3a9d6bd83a7eccaa4282d5166f', 'change': {'files': ['BuildingLink.ShiftLog.Tests/BuildingLink.ShiftLog.Tests.csproj', 'BuildingLink.ShiftLog/BuildingLink.ShiftLog.csproj'], 'comment': 'added StyleCop.Analyzers package into 2 projects\n', 'username': 'oleg', 'vcsRoot': 'ShiftLog'}},
            {'version': '7fa914fe7eb88726e01270d5ea3a1b34ff7a9bc9', 'change': {'files': ['estates/dev/bms/contentcreator/deployment-announcements-sync.yaml', 'estates/dev/bms/contentcreator/deployment-api.yaml', 'estates/dev/bms/contentcreator/deployment-email-send-queue.yaml'], 'comment': 'Updating image\n', 'username': 'teamcity', 'vcsRoot': 'deployments'}},
            {'version': '9ee75a8ab659d85670c0930a1c9f524c35f610ce', 'change': {'files': ['estates/prod/pdc/bms/shiftlog/deployment-api.yaml'], 'comment': 'Updating image\n', 'username': 'teamcity', 'vcsRoot': 'deployments'}},
            {'version': 'b33cce6b568e616221708a9d30e6801d736859a4', 'change': {'files': ['estates/prod/pdc/bms/emergencybroadcast/deployment-emergencybroadcast.yaml'], 'comment': 'Updating image\n', 'username': 'teamcity', 'vcsRoot': 'deployments'}},
            {'version': '94137fd94c40879441f6fd891dfd6e5f9b50ae1b', 'change': {'files': ['estates/dev/bms/emergencybroadcast/deployment-emergencybroadcast.yaml'], 'comment': 'Updating image\n', 'username': 'teamcity', 'vcsRoot': 'deployments'}},
            {'version': '880b5387aefdc70961adce60940628e6d477ca1e', 'change': {'files': ['estates/prod/azure/bms/contentcreator/deployment-announcements-sync.yml', 'estates/prod/azure/bms/contentcreator/deployment-api.yml', 'estates/prod/azure/bms/contentcreator/deployment-email-send-queue.yml'], 'comment': 'Updating image (prod)\n', 'username': 'teamcity', 'vcsRoot': 'deployments'}},
            {'version': '209aaa86e0f5d3c9ed07fe00a66aa23bdcfb49f1', 'change': {'files': ['estates/prod/azure/bfs/retrybatches/configmap-retrybatches.yaml', 'estates/prod/azure/bfs/retrybatches/deployment-retrybatches.yaml', 'estates/prod/azure/bfs/retrybatches/secret-ghcr.yaml', 'estates/prod/azure/bfs/retrybatches/secret-retrybatches.yaml', 'estates/prod/azure/bfs/retryfailedmail/configmap-retryfailedmail.yaml', 'estates/prod/azure/bfs/retryfailedmail/deployment-retryfailedmail.yaml', 'estates/prod/azure/bfs/retryfailedmail/secret-ghcr.yaml', 'estates/prod/azure/bfs/retryfailedmail/secret-retryfailedmail.yaml'], 'comment': 'remaning retryfailedmail\n', 'username': 'dan', 'vcsRoot': 'deployments'}},
            {'version': '6d3f1d943d84d8a8fcab2959b8b9d2c6b8775dda', 'change': {'files': ['estates/dev/bms/shiftlog/deployment-api.yaml'], 'comment': 'Updating image\n', 'username': 'teamcity', 'vcsRoot': 'deployments'}}
        ]
    }
}

if __name__ == "__main__":
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    #app.run(host="127.0.0.1", port=PORT, debug=True)

    msg = {
        "data": base64.b64encode(json.dumps(event).encode("utf-8")).decode("utf-8")
    }
    process_teamcity_event(msg)
