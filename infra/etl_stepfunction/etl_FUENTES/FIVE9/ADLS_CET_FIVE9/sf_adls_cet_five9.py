from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_logs as logs,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as _lambda,
)
from .....utils.naming import create_name


@dataclass
class EtlSfAdlsCetFive9ConstructProps:
    environment: str
    region: str
    lookup_redshift_fn: _lambda.IFunction
    account: str


class EtlSfAdlsCetFive9Construct(Construct):

    def __init__(self, scope: Construct, id: str, props: EtlSfAdlsCetFive9ConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        region = props.region
        lookup_redshift_fn = props.lookup_redshift_fn
        account = props.account

        query_template = r"""declare @fromDate datetime;
declare @toDate datetime;
declare @min int;
declare @reporte varchar(500);

set @reporte='?p1.?p2'
set @min=?p3

set @fromDate =
(
  select max(dateadd(SECOND,1,
    (CONVERT(datetime,substring (case when valorPivot='-' then null else valorPivot end,1,8)) +
     CONVERT(datetime,STUFF(STUFF(cast(substring (case when valorPivot='-' then null else valorPivot end,17,6) as varchar),3,0,':'),6,0,':'))
    )
  ))
  FROM [dbo].[controlPipelineLight_gen2]
  where sistemaFuente='FilesFive9' and nombreTabla=@reporte
);

set @toDate =
(
  select dateadd(SECOND,86399,
    CONVERT(datetime,DATEADD(day, -1,CAST( CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'SA Pacific Standard Time' AS datetime) AS Date )))
  )
);

; with cteNumbers as (
  select row_number() over (order by o1.object_id, o2.object_id) - 1 as rn
  from sys.objects o1 cross join sys.objects o2
)

select
  REPLACE(CONVERT(varchar(20),dateadd(minute, rn * @min, @fromDate),120),' ','T')  as DateStar,
  REPLACE(CONVERT(varchar(20),dateadd(SECOND,-1,dateadd(minute, (rn * @min)+@min, @fromDate)),120),' ','T')  as DateEnd
from cteNumbers
where dateadd(minute, rn * @min, @fromDate) between @fromDate and @toDate

UNION
select a.dateStart, a.dateEnd from (
  select dateStart, dateEnd from (
    select
      cast(cast(substring(case when valorPivot = '-' then null else valorPivot end, 1, 8) as date) as varchar(10)) + 'T' +
      stuff(stuff(cast(substring(case when valorPivot = '-' then null else valorPivot end, 10, 6) as varchar), 3, 0, ':'), 6 ,0, ':') as dateStart,
      cast(cast(substring(case when valorPivot = '-' then null else valorPivot end, 1, 8) as date) as varchar(10)) + 'T' +
      stuff(stuff(cast(substring(case when valorPivot = '-' then null else valorPivot end, 17, 6) as varchar), 3, 0, ':'), 6 ,0, ':') as dateEnd
    from [dbo].[controlPipelineLight_gen2]
    where sistemaFuente = 'FilesFive9' and nombreTabla = @reporte
    group by
      cast(cast(substring(case when valorPivot = '-' then null else valorPivot end, 1, 8) as date) as varchar(10)) + 'T' +
      stuff(stuff(cast(substring(case when valorPivot = '-' then null else valorPivot end, 10, 6) as varchar), 3, 0, ':'), 6 ,0, ':'),
      cast(cast(substring(case when valorPivot = '-' then null else valorPivot end, 1, 8) as date) as varchar(10)) + 'T' +
      stuff(stuff(cast(substring(case when valorPivot = '-' then null else valorPivot end, 17, 6) as varchar), 3, 0, ':'), 6 ,0, ':')
    Having max([estado]) = 0 or max([estado]) = 2
  ) as n
  where dateStart is not null and dateEnd is not null
) as a
left join (
  select
    cast(cast(substring(case when valorPivot = '-' then null else valorPivot end, 1, 8) as date) as varchar(10)) + 'T' +
    stuff(stuff(cast(substring(case when valorPivot = '-' then null else valorPivot end, 10, 6) as varchar), 3, 0, ':'), 6 ,0, ':') as dateStart,
    cast(cast(substring(case when valorPivot = '-' then null else valorPivot end, 1, 8) as date) as varchar(10)) + 'T' +
    stuff(stuff(cast(substring(case when valorPivot = '-' then null else valorPivot end, 17, 6) as varchar), 3, 0, ':'), 6 ,0, ':') as dateEnd
  FROM [dbo].[controlPipelineLight_gen2]
  where sistemaFuente = 'FilesFive9' and nombreTabla = @reporte
    and (valorpivot <> '-' or valorpivot is null)
    and (archivodestino <> '-' or valorpivot is null)
  group by
    cast(cast(substring(case when valorPivot = '-' then null else valorPivot end, 1, 8) as date) as varchar(10)) + 'T' +
    stuff(stuff(cast(substring(case when valorPivot = '-' then null else valorPivot end, 10, 6) as varchar), 3, 0, ':'), 6 ,0, ':'),
    cast(cast(substring(case when valorPivot = '-' then null else valorPivot end, 1, 8) as date) as varchar(10)) + 'T' +
    stuff(stuff(cast(substring(case when valorPivot = '-' then null else valorPivot end, 17, 6) as varchar), 3, 0, ':'), 6 ,0, ':')
) as b
  on a.dateStart = b.dateStart and a.dateEnd = b.dateEnd
where b.dateStart is null and b.dateEnd is null
order by 1 asc"""

        # Step 1 - Set query template
        set_qry_pass = sfn.Pass(
            self,
            "Qry Rango Fecha",
            parameters={
                "folderName.$": "$.folderName",
                "reportName.$": "$.reportName",
                "minutesToInterval.$": "$.minutesToInterval",
                "mail.$": "$.mail",
                "queryTemplate": query_template,
            },
            result_path="$.qryCtx"
        )

        # Step 2 - Invoke lookup Lambda
        lookup_task = tasks.LambdaInvoke(
            self,
            "Lookup1",
            lambda_function=lookup_redshift_fn,
            payload=sfn.TaskInput.from_object({
                "queryTemplate.$": "$.qryCtx.queryTemplate",
                "folderName.$": "$.qryCtx.folderName",
                "reportName.$": "$.qryCtx.reportName",
                "minutesToInterval.$": "$.qryCtx.minutesToInterval"
            }),
            result_selector={"rows.$": "$.Payload.rows"},
            payload_response_only=True,
            result_path="$.lookup"
        )

        # Step 3 - ForEach Map
        for_each = sfn.Map(
            self,
            "ForEach1",
            items_path="$.lookup.rows",
            max_concurrency=1,
            parameters={
                "item.$": "$$.Map.Item.Value",
                "folderName.$": "$.qryCtx.folderName",
                "reportName.$": "$.qryCtx.reportName",
                "mail.$": "$.qryCtx.mail"
            },
            result_path=sfn.JsonPath.DISCARD
        )

        set_date_start = sfn.Pass(
            self, "Set dateStart",
            parameters={"dateStart.$": "$.item.DateStar"},
            result_path="$.dateStartCtx"
        )

        set_date_end = sfn.Pass(
            self, "Set dateEnd",
            parameters={"dateEnd.$": "$.item.DateEnd"},
            result_path="$.dateEndCtx"
        )

        consolidate = sfn.Pass(
            self, "Consolidate payload",
            parameters={
                "folderName.$": "$.folderName",
                "reportName.$": "$.reportName",
                "dateStart.$": "$.dateStartCtx.dateStart",
                "dateEnd.$": "$.dateEndCtx.dateEnd",
                "mail.$": "$.mail"
            },
            result_path="$.execInput"
        )

        child_sm_name = f"dl-general-cet-diario-{environment}-sfn-{region}"
        child_sm_arn = f"arn:aws:states:{region}:{account}:stateMachine:{child_sm_name}"

        start_child = tasks.StepFunctionsStartExecution(
            self,
            "Execute PipelineWebService",
            state_machine=sfn.StateMachine.from_state_machine_arn(self, "ChildStateMachine", child_sm_arn),
            input=sfn.TaskInput.from_json_path_at("$.execInput"),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            name=sfn.JsonPath.format("child-{}-{}", sfn.JsonPath.string_at("$.reportName"), sfn.JsonPath.string_at("$.dateStart"))
        )

        # Parallel step for date start/end, then consolidate and run child
        parallel_steps = sfn.Parallel(self, "ParallelSetDates") \
            .branch(set_date_start) \
            .branch(set_date_end)

        for_each.iterator(parallel_steps.next(consolidate).next(start_child))

        definition = set_qry_pass.next(lookup_task).next(for_each)

        log_group = logs.LogGroup(
            self,
            "StateMachineExecutionLogGroup",
            retention=logs.RetentionDays.ONE_WEEK
        )

        self.state_machine = sfn.StateMachine(
            self,
            "StepFunctionFuentesFive9",
            state_machine_name=f"{create_name('sfn', 'fuentes-adls-cet-five9')}-{region}",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.FATAL,
                include_execution_data=True
            ),
        )

        # Permissions to start child state machine
        self.state_machine.add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution", "states:DescribeExecution", "states:StopExecution"],
                resources=[child_sm_arn]
            )
        )

        # Step 7 - EventBridge triggers
        schedule_patterns = [
            {"name": "DlyTrg_Five9_Lista", "cron": events.Schedule.cron(minute="30", hour="2"), "folderName": "Shared Reports", "reportName": "Reporte lista", "minutesToInterval": "180", "mail": "mauricio.quiroga@bancofinandina.com; andres.alvarado@bancofinandina.com"},
            {"name": "DlyTrg_Five9_Modulo", "cron": events.Schedule.cron(minute="20", hour="2"), "folderName": "Shared Reports", "reportName": "Reporte modulo", "minutesToInterval": "180", "mail": "mauricio.quiroga@bancofinandina.com; andres.alvarado@bancofinandina.com"},
            {"name": "DlyTrg_Five9_Agentes", "cron": events.Schedule.cron(minute="40", hour="2"), "folderName": "Shared Reports", "reportName": "Reporte agentes", "minutesToInterval": "180", "mail": "mauricio.quiroga@bancofinandina.com; andres.alvarado@bancofinandina.com"},
            {"name": "DlyTrg_Five9_Contactos", "cron": events.Schedule.cron(minute="50", hour="2"), "folderName": "Shared Reports", "reportName": "Reporte contactos", "minutesToInterval": "180", "mail": "mauricio.quiroga@bancofinandina.com; andres.alvarado@bancofinandina.com"},
            {"name": "DlyTrg_Five9_LlamadasEstadisticas", "cron": events.Schedule.cron(minute="0", hour="2"), "folderName": "Shared Reports", "reportName": "Reporte llamadas y estadisticas", "minutesToInterval": "180", "mail": "mauricio.quiroga@bancofinandina.com; andres.alvarado@bancofinandina.com"},
            {"name": "DlyTrg_Five9_IVR", "cron": events.Schedule.cron(minute="10", hour="2"), "folderName": "Shared Reports", "reportName": "Reporte IVR", "minutesToInterval": "180", "mail": "mauricio.quiroga@bancofinandina.com; andres.alvarado@bancofinandina.com"},
            {"name": "DlyTrg_Five9_Trafico", "cron": events.Schedule.cron(minute="0", hour="3"), "folderName": "Shared Reports", "reportName": "Trafico Dwh", "minutesToInterval": "180", "mail": "mauricio.quiroga@bancofinandina.com; andres.alvarado@bancofinandina.com"},
        ]

        for table in schedule_patterns:
            events.Rule(
                self,
                f"{table['name']}TriggerRule",
                rule_name=create_name("event-rule", table["name"]),
                schedule=table["cron"],
                targets=[
                    targets.SfnStateMachine(
                        self.state_machine,
                        input=events.RuleTargetInput.from_object({
                            "folderName": table["folderName"],
                            "reportName": table["reportName"],
                            "minutesToInterval": table["minutesToInterval"],
                            "mail": table["mail"]
                        })
                    )
                ]
            )
