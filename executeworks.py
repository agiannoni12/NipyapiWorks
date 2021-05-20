from deployOnProd.updateProcessor import UpdateProcessor

nifiHost = "172.30.215.202"
nifiPort =  "8080"
workingJob = UpdateProcessor(nifiHost=nifiHost,nifiPort=nifiPort)
processgroupid = "b29787db-0178-1000-ffff-ffff99b68d71"
rootid = workingJob.getRoot()
procesadores = workingJob.listarProcesadores(processgroupid)
PYTHONPATH = "/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/python:/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH"
ExtractText = "ExtractText"
UpdateAttribute = "UpdateAttribute"
target = "landingpath"
new_value_on_target = '''${archivo:replaceAll('(.*?([0-9]{8})\.gz)',"/user/admin/${parametros:jsonPath('$.entorno')}/${parametros:jsonPath('$.entidad')}/01-raw/${parametros:jsonPath('$.tipo')}/fecha_proceso=$2/")}'''
new_key_on_target = "raw_path"


workingJob.realizarUpdateExecute(procesadores,"PYTHONPATH",PYTHONPATH)
"""
workingJob.realizarUpdateWhereTarget(
            target=target,
            tipo=UpdateAttribute,
            listaproc=procesadores,
            key=new_key_on_target,
            value=new_value_on_target)

workingJob.realizarUpdateJolt('jolt-spec',"JoltTransformJSON",procesadores,'jolt-spec')

"""

