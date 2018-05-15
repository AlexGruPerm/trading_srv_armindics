import logging
import requests
import json

logging.basicConfig(format = u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', level = logging.DEBUG)

class JsonLoader():
    """ Class just for load json by URL and store it in member jsonContent. """
    serviceUrl  = None  # URL of service
    textContent = None  # text response from service
    jsonContent = None  # json format of response text

    def __init__(self,pUrl):
        """ Constructor. """
        self.serviceUrl = pUrl

    def request_json(self):
        """ Make request and receive response. """
        self.textContent = requests.post(url = self.serviceUrl)
        #print(self.textContent.text.encode('utf-8').decode('utf-8'))
        self.jsonContent = json.loads(self.textContent.text)#.encode('utf-8').decode('utf-8')

    def get_json(self):
        """ Return downloaded jsonContent as <class 'list'> """
        return self.jsonContent


class JsonParser():
    """ Class for parsing json with defined strcuture """
    jsonContent = None  # json content for parsing

    def __init__(self,pJsonContent):
        """ Constructor. """
        self.jsonContent = pJsonContent

    def save_into_db(self):
        """ Parse json content and save into db  """


    def outputJson(self):
        """ Output json content for debug purpose """
        i=0
        #elm is a dict from get_json - list of dicts.
        for elm in self.jsonContent:
            print("------------------------------------------------------------------------------------------------")
            elm_id   = elm['id']
            elm_name = elm['name']
            elm_indicators = elm['indicators']
            print("basic: "+str(elm_id)+" "+elm_name+" "+" count="+ str(len(elm_indicators)))
            # ----- indicators -------------
            for pv in elm_indicators:
                pv_id = pv['id']
                pv_name = pv['name']
                pv_isInProgress = pv['isInProgress']
                pv_dateInProgress = pv['dateInProgress']
                pv_hint = pv['hint']
                pv_date = pv['date']
                pv_extraInfo = pv['extraInfo']
                pv_valueGroups = pv['valueGroups']
                print("      indicators:" + str(pv_id) +"  "+ pv_name )
                # ----- value groups -------------
                for vg in pv_valueGroups:
                    vg_id     = vg['id']
                    vg_name   = vg['name']
                    vg_values = vg['values']
                    print("            " + str(vg_id) + "  " + vg_name )
                    # ----- values -------------
                    for vl in vg_values:
                        vl_type = vl['type']
                        vl_typeName = vl['typeName']
                        vl_units = vl['units']
                        vl_value = vl['value']
                        vl_percent = vl['percent']
                        vl_dynamicsVector = vl['dynamicsVector']
                        vl_dinamics = vl['dinamics']
                        print("                    valueGroups:" + str(vl_type) + "  " + vl_typeName) # + " " + str(vl_dinamics))
                        # ----- dinamics -------------
                        for dm in vl_dinamics:
                            dm_period = dm['period']
                            dm_value = dm['value']
                            dm_date = dm['date']
                            dm_dynamicsVector = dm['dynamicsVector']
                            print("                           dinamics:" + dm_period + "  " + str(dm_value)+ "  "+ dm_date + "  " + dm_dynamicsVector)

            i+=1




def main():
    """Enter point."""
    serviceUrl = "http://87.245.154.49/trading/service/new/armIndicators"

    srv = JsonLoader(serviceUrl)
    srv.request_json()

    prs = JsonParser(srv.get_json()) # input - list of dicts
    prs.outputJson()





main()
