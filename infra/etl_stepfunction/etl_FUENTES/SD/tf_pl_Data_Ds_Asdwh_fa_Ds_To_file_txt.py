# pl_txt_tf.py
from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    aws_sns as sns,
    aws_sns_subscriptions as subs,
)
from ....utils.naming import create_name

@dataclass
class PlTxtTfProps:
    """
    Props para PlTxtTf:
      - environment: entorno (p. ej. 'dev', 'prod')
      - alert_emails: lista opcional de correos a suscribir al tópico
      - topic_name_override: nombre explícito opcional del tópico SNS
    """

    environment: str
    alert_emails: list[str] | None = None
    topic_name_override: str | None = None


class PlTxtTf(Construct):
    """
    Crea un tópico SNS para notificar fallas en la pipeline
    pl-Data-Ds-Asdwh-fa-Ds-To-file-txt (SP/UNLOAD).
    """

    def __init__(self, scope: Construct, id: str, props: PlTxtTfProps) -> None:
        super().__init__(scope, id)

        # Nombre del tópico: usa override si se pasa, si no usa default
        topic_name = (
            props.topic_name_override
            or create_name("sns", "pl-Data-Ds-Asdwh-fa-Ds-To-file-txt-notify") # f"pl-Data-Ds-Asdwh-fa-Ds-To-file-txt-{props.environment}"
        )

        # Crear el tópico SNS
        topic = sns.Topic(
            self,
            "FailureTopic",
            topic_name=topic_name,
            display_name="Fallas - pl_Data_Ds_Asdwh_fa_Ds_To_file_txt (SP/UNLOAD)",
        )

        # Suscribir correos si hay
        for mail in props.alert_emails or []:
            topic.add_subscription(subs.EmailSubscription(mail))

        # Propiedad pública
        self.failure_topic = topic
