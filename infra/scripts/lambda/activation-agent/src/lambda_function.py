import http.client
import time

MAX_RETRIES = 20
RETRY_DELAY_S = 10

def lambda_handler(event, context):
    """
    CloudFormation Custom Resource handler.
    - Create: intenta obtener ActivationKey desde el agente DataSync.
    - Update: no-op (retorna éxito).
    - Delete: éxito inmediato (no bloquear destrucción).
    """
    request_type = event.get("RequestType", "Create")
    physical_id = event.get("PhysicalResourceId") or "DataMigrationActivationResource"

    # ---- DELETE: responder rápido y sin errores ----
    if request_type == "Delete":
        return {"PhysicalResourceId": physical_id}

    # ---- UPDATE: no-op seguro (evita esperas innecesarias) ----
    if request_type == "Update":
        return {"PhysicalResourceId": physical_id}

    # ---- CREATE: lógica de activación ----
    props = event.get("ResourceProperties", {})
    ip = props.get("agentIpAddress")
    region = props.get("activationRegion")

    if not ip or not region:
        raise ValueError("Missing required properties: 'agentIpAddress' and 'activationRegion'")

    path = f"/?gatewayType=SYNC&activationRegion={region}&no_redirect"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Intento {attempt}: conectando a http://{ip}{path}")
            conn = http.client.HTTPConnection(ip, timeout=5)
            conn.request("GET", path)
            response = conn.getresponse()
            if response.status == 200:
                key = response.read().decode().strip()
                print(f"ActivationKey obtenido: {key}")
                return {
                    "PhysicalResourceId": ip,
                    "Data": {"ActivationKey": key}
                }
            else:
                raise Exception(f"HTTP {response.status}")
        except Exception as e:
            print(f"Intento {attempt} falló: {e}")
            if attempt == MAX_RETRIES:
                # Propaga error a CFN si agotamos reintentos en CREATE
                raise
            time.sleep(RETRY_DELAY_S)
