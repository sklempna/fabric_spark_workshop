# Fabric notebook source


# CELL ********************

%pip install semantic-link

import sempy.fabric as fabric

# CELL ********************

# list workspaces

fabric.list_workspaces()

# CELL ********************

# list items in current workspace

dev_items = fabric.list_items()
dev_items

# CELL ********************

# list items in prod workspace

dev_ws_id = '5b0eaee0-12a4-4652-93b8-0f08395fb823'
prod_ws_id = '505f9304-b9af-46ff-a594-82c9d56731ad'


# help(fabric.list_items)
prod_items = fabric.list_items(workspace=prod_ws_id)
prod_items

# CELL ********************

# get dataframe of ids for the same objects

df = dev_items.merge(prod_items, on=["Display Name", "Type"], how="inner") \
        .rename(columns={"Display Name": "name", "Id_x": "dev_id", "Id_y": "prod_id", "Type": "type"})[["name", "type", "dev_id", "prod_id"]]
df

# CELL ********************

dev_to_prod = {}

for row in df[["dev_id", "prod_id"]].itertuples():
    translator_dict[row.dev_id]

# CELL ********************

import json

client = fabric.FabricRestClient()

url = f"v1/workspaces/{dev_ws_id}/items"


# CELL ********************

json.loads(client.get(url).text)

# CELL ********************

item_id = '36790f46-5ab6-48ce-bc5d-c3708bf0d41f' # pipeline
# item_id = 'f54fe52f-6e2a-43b7-b56c-8b3ba30e4c4e' # notebook
# item_id = 'e423e6d1-ea68-49a2-911c-38fdf3b4e3db' # lakehouse

notebook_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition"

d = json.loads(client.post(notebook_url).text)

# CELL ********************

from base64 import b64decode

s = b64decode(d.get('definition').get('parts')[0].get('payload'))

# CELL ********************

defn = d.get('definition')

# CELL ********************

create_item_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"

body = {
    "displayName": "testpipelinewullu",
    "type": "DataPipeline",
    "definition": defn,
    "description": "this is not a test, this is rocknroll"
}

r = client.post(create_item_url, json=body)

# CELL ********************

r

# CELL ********************

