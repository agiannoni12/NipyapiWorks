from nipyapi import *

class Start():

    def __init__(self,nifiHost,nifiPort ):
        config.nifi_config.host = "http://{}:{}/nifi-api".format(nifiHost,nifiPort)
        config.registry_config.host = "http://{}:1{}/nifi-registry-api".format(nifiHost,nifiPort)

    def getRoot(self):
        self.rootid = canvas.get_root_pg_id()
        return self.rootid

    def listarProcesadores(self,pgid):
        self.procesadores = canvas.list_all_processors(pgid)
        return self.procesadores

    def listarTemplates(self):
        self.templates = templates.list_all_templates()
        return self.templates

    def getTemplate(self,tempid):
        self.template = templates.get_template(tempid)
        return self.template

