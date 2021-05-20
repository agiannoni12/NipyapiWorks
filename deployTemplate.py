from nipyapi import *

config.nifi_config.host = 'http://172.30.215.202:8080/nifi-api'  # 'http://localhost:8080/nifi-api'


root = canvas.get_root_pg_id()
#para sacar los uuid de los templates se hace templates.list_all_templates()

LogOnRaw = '0b126698-f95f-3922-ba32-4d5000279155'
LogOnLanding = '40992dc0-dd75-3175-8a52-d866e10d7f8f'
ListaPipelinesBSJ = ["b35c933e-0178-1000-ffff-ffff96ee7bcb","b35cfc48-0178-1000-ffff-ffffd3d22730","b35cc673-0178-1000-ffff-ffffdc7cf3ec"]
ListaPipelinesBER = ["b363393c-0178-1000-0000-00001cc6360c","b3639ba7-0178-1000-0000-00007cfdeee0","b3630d7c-0178-1000-ffff-ffffe9cd8556"]
ListaPipelinesBSF = ["b662835b-0178-1000-0000-00000755ccb1","b661611b-0178-1000-0000-00005e8a95dd","b6625f8d-0178-1000-0000-000047b11dd5"]
ListaPipelinesBSC = ["b6672a3a-0178-1000-0000-00003bb611d8","b666b6f9-0178-1000-0000-00002465c3e5","b6668c66-0178-1000-ffff-ffffa28a33d8"]

def recorrerPipes(pipe):
    flowpadre = canvas.recurse_flow(pipe)
    flowshijos = flowpadre.process_group_flow.flow.process_groups
    flowsnietos = []

    for x in flowshijos:
        flowsnietos.append(x.component.id)
    return flowsnietos

def deployarTemplate(pipes):
    flowpadre = canvas.recurse_flow(pipes)
    flowshijos = flowpadre.process_group_flow.flow.process_groups
    for x in flowshijos:
        if x.component.name == "Transform":
            templates.deploy_template(x.component.id, LogOnRaw, 400, 400)
            templates.deploy_template(x.component.id, LogOnLanding, 600, 600)
            print("ok")

for pg in ListaPipelinesBSF:
    pipeGeneral = recorrerPipes(pg)
    for pipe in pipeGeneral:
        deployarTemplate(pipe)
print("ok para BSF")


for pg in ListaPipelinesBER:
    pipeGeneral = recorrerPipes(pg)
    for pipe in pipeGeneral:
        deployarTemplate(pipe)
print("ok para BER")


for pg in ListaPipelinesBSJ:
    pipeGeneral = recorrerPipes(pg)
    for pipe in pipeGeneral:
        deployarTemplate(pipe)
print("ok para BSJ")


for pg in ListaPipelinesBSC:
    pipeGeneral = recorrerPipes(pg)
    for pipe in pipeGeneral:
        deployarTemplate(pipe)
print("ok para BSC")