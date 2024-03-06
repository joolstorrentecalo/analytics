import os
import requests
from logging import info

dwId = os.environ.get("CI_DATA_WAREHOUSE_ID")
x_mcd_id = os.environ.get("CI_MCD_TOKEN_ID")
x_mcd_token = os.environ.get("CI_MCD_TOKEN_SECRET")
url='https://api.getmontecarlo.com/graphql'


headers = {
    'x-mcd-id': x_mcd_id,
    'x-mcd-token': x_mcd_token,
    'Content-Type': 'application/json'
}

def get_response(json):
    """
    Return response object from Monte Carlo API
    """
    response = requests.post(url, headers=headers, json=json)
    response_content = response.json()
    return response_content

def get_table_path_query(tableId):
    """
    Return table path based on table name
    i.e table_name='instance_redis_metrics'

    For this particular use case, it will return the full_table_path i.e, raw:saas_usage_ping.instance_redis_metrics
    """
    first = 1
    json={
            'query': 'query GetTables($dwId:UUID,$tableId:String,$first:Int) {getTables(dwId:$dwId,tableId:$tableId,first:$first) {edges{node{mcon,fullTableId}}}}',
            'variables': {"dwId":f"{dwId}","tableId":f"{tableId}", "first":f"{first}"}
        }
    response_content = get_response(json)
    full_table_path = response_content["data"]["getTables"]["edges"][0]["node"]["fullTableId"]
    return full_table_path

def query_table(fullTableId):
    '''
    Return table information based on full_table_path
    i.e full_table_path='raw:saas_usage_ping.instance_redis_metrics'

    For this particular use case, used to return the table mcon (monte carlo table id)
    '''
    json={
        'query': 'query GetTable($dwId: UUID,$fullTableId: String){getTable(dwId:$dwId,fullTableId:$fullTableId){tableId,mcon}}',
        'variables':{"dwId":f"{dwId}","fullTableId":f"{fullTableId}"}
    }

    response_content = get_response(json)
    table_mcon = response_content["data"]["getTable"]["mcon"]

    return table_mcon

def get_downstream_node_dependencies(source_table_mcon):
    """
    This will return all directly dependent downstream tableau nodes for a given model.
    """
    direction="downstream"
    json={
        'query': 'query GetTableLineage($direction: String!, $mcon: String) {getTableLineage(direction:$direction,mcon:$mcon){connectedNodes{displayName,mcon,objectType}}}',
        'variables': {"direction": f"{direction}","mcon":f"{source_table_mcon}"}
    }

    response_content = get_response(json)
    response_derived_tables_partial_lineage = response_content["data"]["getTableLineage"]

    return response_derived_tables_partial_lineage

def check_response_for_tableau_dependencies(response_downstream_node_dependencies):
    """
    This will return all dependent downstream nodes for a given source table.
    """
    output_list=[]
    for node in response_downstream_node_dependencies["connectedNodes"]:
        output_dict = {}
        object_type=['tableau-published-datasource-live', 'tableau-published-datasource-extract', 'tableau-view']
        if node['objectType'] in object_type and node['objectType'] != 'periscope-chart':
            #append node to output_dict
            output_dict[node['displayName']] = f"https://getmontecarlo.com/assets/{node['mcon']} ({node['objectType']})"
            #append output_dict to output_list
            output_list.append(output_dict)
    
    return output_list

# Assumes git diff was run to output the sql files that changed
with open("diff.txt", "r") as f:
    lines = f.readlines()
    for line in lines:
        info("Checking for downstream dependencies in Tableau for the model " + line.strip() + "")
        full_table_path = get_table_path_query(line)
        # if no path is returned exit the script
        source_table_mcon = query_table(full_table_path)
        #if no mcon is found raise a status saying no mcon detected and exit
        response_downstream_node_dependencies = get_downstream_node_dependencies(source_table_mcon)
        output_list = check_response_for_tableau_dependencies(response_downstream_node_dependencies)

        # if length of output_list is greater then zero then show the list of downstream dependencies
        if len(output_list) > 0:
            # show each key value pair in output_list and append them in comparison.txt
            with open("comparison.txt", "a") as f:
                write_string = f"\n\ndbt model: {line}\nFound {len(output_list)} downstream dependencies in Tableau for the model {line.strip()}\n"
                f.write(write_string)
                for item in output_list:
                    for key, value in item.items():
                        f.write(f"\n{key}: {value}")
