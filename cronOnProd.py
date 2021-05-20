from nipyapi import *
import pandas as p
from deployOnProd.main import Start

class CronOnProd(Start):

    def __init__(self,nifiHost,nifiPort):
        super().__init__(nifiHost,nifiPort)

#procesador = canvas.get_processor("4cac3058-726b-1627-a3a5-d5db2205238d", "id").component

    error = []
    def cronometrar (self, pg):
        self.lista = []

        for proc in pg:
            try:
                estrategia = proc.component.config.scheduling_strategy

                if estrategia:
                    #print(estrategia)
                    #print(proc)
                    horario = proc.component.config.scheduling_period

                    parentgroupid = proc.component.parent_group_id

                    nombrepg = canvas.get_process_group(parentgroupid,"id").component.name

                    padreDelpg = canvas.get_process_group(parentgroupid,"id").component.parent_group_id
                    #print(padreDelpg)
                    nombrePadre = canvas.get_process_group(padreDelpg,"id").component.name
                    #print(nombrePadre)
                    self.info = "{}|{}|{}|{}".format(estrategia, horario, nombrePadre, nombrepg)
                    print(self.info)
                    self.lista.append(self.info)
            except Exception as e:
                #print(e)
                continue
        return self.lista

    def armar_csv (self, lista):
        with open("crons.csv", "w") as pr:
            pr.write("Tipo|Horario|processGroup|subProcessGroup\n")
            for x in lista:
                pr.write("{}\n".format(x))
        return "crons.csv"


    def armar_excel(self, file):
        csv=p.read_csv(file,header=None, sep="|")
        csv.to_excel("NiFi_crons.xls", index=False)


workingJob = CronOnProd("172.30.215.202","8080")

rootid = workingJob.getRoot()

procesadores = workingJob.listarProcesadores("b6625f8d-0178-1000-0000-000047b11dd5")

crons = workingJob.cronometrar(procesadores)

print(crons)



