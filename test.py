from nipyapi import *
import json
import pandas as p

nifiHost,nifiPort = "172.30.215.202" , "8080"


config.nifi_config.host = "http://{}:{}/nifi-api".format(nifiHost, nifiPort)
config.registry_config.host = "http://{}:1{}/nifi-registry-api".format(nifiHost, nifiPort)

pgid = canvas.get_root_pg_id()

procesadores = list(filter(lambda x:"ExecuteStreamCommand" in x.component.type,canvas.list_all_processors("ace10608-dce0-3bf9-ac82-ad0999fc7459")))
#procesadores=canvas.list_all_processors("367c4f1d-a3e4-338d-b00f-4232f0753330")

for x in procesadores:
    propiedades = x.component.config.properties

    valorComPath = propiedades["Command Path"]
    if "python" in valorComPath:
        print(valorComPath)
        propiedades["Command Path"]="python"
    try:
        conf = nifi.ProcessorConfigDTO(properties=propiedades)
        canvas.update_processor(x, conf)
        print("ok")
    except ValueError:
        print("salteado")
        continue







"""
procesadores = filter(lambda x:
                        "JoltTransformJSON" in x.component.type
                        and "accion" in x.component.config.properties['jolt-spec']
                      ,
                      canvas.list_all_processors(pg_id=pgid))


acciones = set()
for x in procesadores:
    print(x.component.config.properties['jolt-spec'])
    try:
        jolt = json.loads(x.component.config.properties['jolt-spec'])
        accion = jolt.get("accion")
        acciones.add(accion)
        print(accion)
    except:
        continue
print(acciones)



successes = filter(lambda x:x.component.name=="Success" ,canvas.list_all_process_groups(pgid))

query = "SELECT count(1) FROM ${parametros:jsonPath('$.tabla_raw')} where cast(fecha_proceso as bigint) = ${parametros:jsonPath('$.fecha_proceso')}"
conf = {'properties': {"SQL select query": query}}

for x in successes:
    canvas.create_processor(x,canvas.get_processor_type("ExecuteSQL")[0],(100,100),"SelectAImpala", config=conf)
    print("ok")
"""
