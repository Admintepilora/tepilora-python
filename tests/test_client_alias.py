from Tepilora import Client, TepiloraClient
from Tepilora.client import Client as ModuleClient


def test_client_alias_points_to_tepilora_client():
    assert Client is TepiloraClient
    assert ModuleClient is TepiloraClient
