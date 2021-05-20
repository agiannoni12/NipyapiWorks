from nipyapi import *
from deployOnProd.main import Start
import json

nifiHost = "172.30.213.201"
nifiPort =  "8080"
workingJob = Start(nifiHost=nifiHost,nifiPort=nifiPort)

root = workingJob.getRoot()

print(root)

procesadores = list(filter(lambda x:"JoltTransform" in x.component.type, workingJob.listarProcesadores(root)))
listadirs=[]

for p in procesadores:
    propiedades = p.component.config.properties
    jolt =  propiedades.get('jolt-spec')
    if "home/goa" in jolt:
        listadirs.append(jolt)

#print(listadirs)
archivodirectorios = open("directoriosdesa.txt", "w")
for x in listadirs:
    try:
        directorioJson = json.loads(x)
        directorio = dict(directorioJson).get("directorio")
        archivodirectorios.write(f"{directorio}\n")
    except:
        continue
archivodirectorios.close()

