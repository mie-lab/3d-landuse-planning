import requests
import sys


def create_namespace(endpoint_url, namespace_name):
    """
    Creates a Blazegraph namespace with quad-store configuration.
    """
    create_url = f"{endpoint_url.rstrip('/')}"

    namespace_config = f'''
    com.bigdata.rdf.sail.truthMaintenance=false
    com.bigdata.rdf.store.AbstractTripleStore.textIndex=true
    com.bigdata.rdf.store.AbstractTripleStore.justify=false
    com.bigdata.rdf.store.AbstractTripleStore.statementIdentifiers=false
    com.bigdata.rdf.store.AbstractTripleStore.axiomsClass=com.bigdata.rdf.axioms.NoAxioms
    com.bigdata.rdf.sail.namespace={namespace_name}
    com.bigdata.rdf.store.AbstractTripleStore.quads=true
    com.bigdata.rdf.store.AbstractTripleStore.geoSpatial=true
    com.bigdata.journal.Journal.groupCommit=false
    com.bigdata.rdf.sail.isolatableIndices=false
    '''

    headers = {"Content-Type": "text/plain"}

    print(f"Creating namespace '{namespace_name}' at {create_url} ...")
    response = requests.post(create_url, headers=headers, data=namespace_config)
    if response.status_code == 201:
        print(f"Namespace '{namespace_name}' created successfully.")
    elif response.status_code == 409:
        print(response.text)
    else:
        print(f"Failed to create namespace '{namespace_name}'. "
              f"HTTP {response.status_code}, Response: {response.text}")
        sys.exit(1)


def upload_nquads(sparql_endpoint, nquads_file):
    """
    Upload an N-Quads file to the given namespace.
    """
    headers = {"Content-Type": "application/n-quads"}

    print(f"Uploading '{nquads_file}' to namespace '{sparql_endpoint}' ...")
    with open(nquads_file, "rb") as f:
        response = requests.post(sparql_endpoint, headers=headers, data=f)

    if response.status_code in [200, 204]:
        print(f"Data successfully uploaded to '{sparql_endpoint}'.")
    else:
        print(f"Upload failed. HTTP {response.status_code}, Response: {response.text}")
        sys.exit(1)
