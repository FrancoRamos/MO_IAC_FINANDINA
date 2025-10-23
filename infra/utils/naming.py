from .environments import environments

config = 'dev'

context_env = environments[config]
region = context_env.region
environment = context_env.environment
account_id = context_env.accountId
project = context_env.project


def create_name(service: str, functionality: str) -> str:
    return f"{project}-{environment}-{region}-{functionality}-{service}-fr".lower()