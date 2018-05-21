# -*- coding: utf-8 -*-
import logging
import requests
import json
import cx_Oracle
import os

os.environ["NLS_LANG"] = "RUSSIAN_RUSSIA.AL32UTF8"
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
    conn = None # connection to oracle RDBMS
    hist_id = None #ID from table LOAD_DATES_HISTORY for relating rows in table TMP_SOURCE_DATA_SERV_11

    def __init__(self,pJsonContent):
        """ Constructor. """
        self.jsonContent = pJsonContent

    def save_into_db(self,
                     p_ID,
                     p_NAME,
                     p_indicators_id,
                     p_indicators_name,
                     p_indicators_isInProgress,
                     p_indicators_dateInProgress,
                     p_indicators_hint,
                     p_indicators_date,
                     p_indicators_extraInfo,
                     p_valueGroups_id,
                     p_valueGroups_name,
                     p_values_type,
                     p_values_typeName,
                     p_values_units,
                     p_values_value,
                     p_values_percent,
                     p_values_dynamicsVector,
                     p_dinamics_period,
                     p_dinamics_value,
                     p_dinamics_date,
                     p_dinamics_dynamicsVector
                     ):
        """ Parse json content and save into db  """
        cur = self.conn.cursor()
        sql = 'INSERT INTO TMP_SOURCE_DATA_SERV_11(ID_LOAD_DATES_HISTORY,ID,NAME, '\
              ' indicators_id,indicators_name,indicators_isInProgress,indicators_dateInProgress,indicators_hint,indicators_date,indicators_extraInfo,'\
              ' valueGroups_id,valueGroups_name,'\
              ' values_type,values_typeName,values_units,values_value,values_percent,values_dynamicsVector,'\
              ' dinamics_period,dinamics_value,dinamics_date,dinamics_dynamicsVector)'\
              ' VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22)'
        cur.execute(sql, (
                          self.hist_id,
                          p_ID,
                          p_NAME,
                          p_indicators_id,
                          p_indicators_name,
                          p_indicators_isInProgress,
                          p_indicators_dateInProgress,
                          p_indicators_hint,
                          p_indicators_date,
                          p_indicators_extraInfo,
                          p_valueGroups_id,
                          p_valueGroups_name,
                          p_values_type,
                          p_values_typeName,
                          p_values_units,
                          p_values_value,
                          p_values_percent,
                          p_values_dynamicsVector,
                          p_dinamics_period,
                          p_dinamics_value,
                          p_dinamics_date,
                          p_dinamics_dynamicsVector))

    def outputJson(self, save_into_db = False):
        """ Output json content for debug purpose """
        con = None
        if save_into_db:
            print(cx_Oracle.version)
            self.conn = cx_Oracle.connect('MSK_ARM_LEAD/MSK_ARM_LEAD@10.127.24.11/test')
            self.conn.autocommit = True
            print("Oracle connection OPENED. Version:"+self.conn.version)
            cur = self.conn.cursor()
            p_hist_id = cur.var(cx_Oracle.NATIVE_INT)
            sql = 'insert into LOAD_DATES_HISTORY(LOAD_DATE,ID_SERVICE_SOURCE) values(sysdate,:source_id) returning ID into :ins_id'
            cur.execute(sql, source_id=11, ins_id=p_hist_id)
            self.hist_id = p_hist_id.getvalue()
            print("-!- self.hist_id = ["+str(self.hist_id)+"]")
            cur.close()

        i=0
        #elm is a dict from get_json - list of dicts.
        for elm in self.jsonContent:
            print("------------------------------------------------------------------------------------------------")
            elm_id   = elm['id']
            elm_name = elm['name']
            elm_indicators = elm['indicators']
            print("basic: "+str(elm_id)+" "+elm_name+"  count="+ str(len(elm_indicators)))
            print(str(elm_indicators))
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
                print("      indicators:" + str(pv_id) + " ["+ str(pv_dateInProgress) +"] " +"  "+ pv_name +" ["+ str(pv_date)+"] -"+str(pv_extraInfo))
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
                        print("                    valueGroups:" + str(vl_type) +" ("+str(vl_value)+ ")  " + vl_typeName + " ("+ vl_units +") " + str(vl_percent) + " " +str(vl_dynamicsVector)) # + " " + str(vl_dinamics))
                        # ----- dinamics -------------
                        for dm in vl_dinamics:
                            dm_period = dm['period']
                            dm_value = dm['value']
                            dm_date = dm['date']
                            dm_dynamicsVector = dm['dynamicsVector']
                            print("                           dinamics:" + dm_period + "  " + str(dm_value)+ "  "+ dm_date + "  " + dm_dynamicsVector)
                            self.save_into_db(
                                              p_ID = elm_id,
                                              p_NAME = elm_name,
                                              p_indicators_id = pv_id,
                                              p_indicators_name = pv_name,
                                              p_indicators_isInProgress = pv_isInProgress,
                                              p_indicators_dateInProgress = pv_dateInProgress,
                                              p_indicators_hint = pv_hint,
                                              p_indicators_date = pv_date,
                                              p_indicators_extraInfo = pv_extraInfo,
                                              p_valueGroups_id = vg_id,
                                              p_valueGroups_name = vg_name,
                                              p_values_type = vl_type,
                                              p_values_typeName = vl_typeName,
                                              p_values_units = vl_units,
                                              p_values_value = vl_value,
                                              p_values_percent = vl_percent,
                                              p_values_dynamicsVector = vl_dynamicsVector,
                                              p_dinamics_period = dm_period,
                                              p_dinamics_value = dm_value,
                                              p_dinamics_date = dm_date,
                                              p_dinamics_dynamicsVector = dm_dynamicsVector
                            )
                            self.conn.commit()
            i+=1
        if not(self.conn is None):
            # after loading data, call parse function in DB.
            cur = self.conn.cursor()
            l_parse_res =  cur.var(cx_Oracle.STRING)
            cur.callproc("PKG_LOAD_PARSE_SERVICE_DATA.save_data_service_11",
                                   [self.hist_id, l_parse_res])
            print("------------------------------------------------------------------------------------")
            print("self.hist_id=["+str(self.hist_id)+"] res=["+l_parse_res.getvalue()+"]")
            print("--------------------------------------------------------")
            cur.close()
            self.conn.close()
            print("Oracle connection CLOSED.")


class DetailsParser():
    """ Class for parsing json with defined strcuture """
    jsonContent = None  # json content for parsing
    conn = None # connection to oracle RDBMS
    hist_id = None #ID from table LOAD_DATES_HISTORY for relating rows in table TMP_SOURCE_DATA_SERV_11

    def __init__(self,pJsonContent):
        """ Constructor. """
        self.jsonContent = pJsonContent

    def outputJson(self, save_into_db = False):
        """ Output json content (details) for debug purpose """
        i=0
        print(self.jsonContent)
        #elm is a dict from get_json - list of dicts.
        for elm in self.jsonContent:
            print("------------------------------------------------------------------------------------------------")
            elm_id      = elm['id']
            elm_titles  = elm['titles']
            elm_details = elm['details']
            #+str(elm_titles)
            print("1)["+str(elm_id)+"] "+str(elm_details))
            for det in elm_details:
                det_id      = det['id']
                det_child = det['children']
                print("   2)["+str(det_id)+"] "+ str(det_child))
                #for det_ch in det_child:
                #    det_ch_id =
                #    det_ch_=






def main():
    """Enter point."""
    # Service main data
    serviceUrl = "http://87.245.154.49/trading/service/new/armIndicators"
    # Service details data
    detailsUrl = "http://87.245.154.49/trading/service/new/armSimpleDetails"

    #data
    """
    srv = JsonLoader(serviceUrl)
    srv.request_json()
    prs = JsonParser(srv.get_json()) # input - list of dicts
    prs.outputJson(save_into_db=True)
    """

    #details

    det = JsonLoader(detailsUrl)
    det.request_json()
    detPars = DetailsParser(det.get_json())
    detPars.outputJson(save_into_db=False)
    print("Finish")






main()
