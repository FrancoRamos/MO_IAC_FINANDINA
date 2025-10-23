from dataclasses import dataclass

@dataclass
class Environment:
    region: str
    environment: str
    project: str
    ownerAccount: str
    appRepo: str
    appBranch: str
    accountId: str


environments = {
    "dev": Environment(
        region="us-east-1",
        environment="dev",
        project="dl",
        ownerAccount="finandina",
        appRepo="cdk-py-finandina",
        appBranch="dev",
        accountId="637423369807",
    ),
    # "staging": Environment(
    #     region="us-east-1",
    #     environment="staging",
    #     project="dl",
    #     ownerAccount="finandina",
    #     appRepo="cdk-py-finandina",
    #     appBranch="staging",
    #     accountId="109398295595",
    # ),
    # "prod": Environment(
    #     region="us-east-1",
    #     environment="prod",
    #     project="dl",
    #     ownerAccount="finandina",
    #     appRepo="cdk-py-finandina",
    #     appBranch="prod",
    #     accountId="109398295595",
    # ),
}
