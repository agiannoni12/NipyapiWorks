from nipyapi import *
from deployOnProd.main import Start

class UpdateProcessor(Start):

    def __init__(self,nifiHost,nifiPort):
        super().__init__(nifiHost,nifiPort)


    def realizarUpdateExecute(self,listaproc,key,value ):
        for p in listaproc:
            if "ExecuteStreamCommand" in p.component.type:
                propiedades = p.component.config.properties
                propiedades[key] = value
                try:
                    conf = nifi.ProcessorConfigDTO(properties=propiedades)
                    canvas.update_processor(p,conf)
                    print("ok")
                except ValueError:
                    print("salteado")
                    continue

    def realizarUpdateProcesador(self,tipo,listaproc,key,value ):
        for p in listaproc:
            if tipo in p.component.type:
                propiedades = p.component.config.properties
                propiedades[key] = value
                try:
                    conf = nifi.ProcessorConfigDTO(properties=propiedades)
                    canvas.update_processor(p,conf)
                    print("ok")
                except ValueError:
                    print("salteado")
                    continue

    def realizarUpdateWhereTarget(self,target,tipo,listaproc,key,value ):
        for p in listaproc:

            if tipo in p.component.type:
                propiedades = p.component.config.properties
                if target in propiedades:
                    propiedades[key] = value
                try:
                    conf = nifi.ProcessorConfigDTO(properties=propiedades)
                    canvas.update_processor(p,conf)
                    print("ok")
                except ValueError:
                    print("salteado")
                    continue

    def realizarUpdateJolt(self,target,tipo,listaproc,key ):
        for p in listaproc:

            if tipo in p.component.type:
                propiedades = p.component.config.properties

                jolt = propiedades.get('jolt-spec')
                if "tabla_raw" in jolt:
                    texto = ''',"raw_path" : "${raw_path}"}'''
                    jolt = jolt[:-1] + texto
                    propiedades[key] = jolt
                try:
                    conf = nifi.ProcessorConfigDTO(properties=propiedades)
                    canvas.update_processor(p,conf)
                    print("ok")
                except ValueError:
                    print("salteado")
                    continue




