from influxdb import InfluxDBClient
import json
from modules import app, cbpi
from datetime import datetime

"""

"""
isChanged = False
ferm_target_temp_old = []
ferm_auto_state_old = []
act_cool_state_old = []
act_heat_state_old = []
sen_1_temp_old = []
sen_2_temp_old = []
sen_3_temp_old = []
    

@cbpi.initalizer(order=100)
def init(cbpi):
    date_time = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    f = open("./logs/INFLDB.log",'w')
    f.write(date_time + " InluxDB Log init ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" + '\n')
    f.close
    

def log(s):
    date_time = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    f = open("./logs/INFLDB.log",'a')
    print(s)
    f.write(date_time + " " + s + '\n')
    f.close    


@cbpi.backgroundtask(key='influxdb_client', interval=10.0)
def influxdb_client_background_task(api):
    
    log("Start BackgroundTask ##########################################################################")
    #Setup database
    #log("Connect to DB.....")
    client = InfluxDBClient('myIP', 8086, 'myInfluxDB', 'myDBUser', 'myDBPassword')
    #client.create_database('CBPiChiller')
    #client.get_list_database()
    #log("switch to DB....")
    client.switch_database('CBPiChiller')
    global isChanged
    global ferm_target_temp_old
    global ferm_auto_state_old
    global act_cool_state_old
    global act_heat_state_old
    global sen_1_temp_old
    global sen_2_temp_old
    global sen_3_temp_old
    
    sensors = cbpi.cache.get('sensors')
    actors = cbpi.cache.get('actors')
    fermenter = cbpi.cache.get('fermenter')
    
    sdata = {}
    for i, svalue in sensors.iteritems():
        #log("INFL Sensors Dict: " + str(svalue.__dict__))
        #log("INFL Sensors Instance Dict: " + str(svalue.instance.__dict__))

        sdata[i] = {
            'id': svalue.instance.id,
            'name': svalue.name,
            'value': svalue.instance.last_value
            #'unit': svalue.instance.get_unit()
            }
        #log("INFL Sensors Data: " + str(sdata[i].get('id')))
    #log("INFL Sensors SData: " + str(sdata))
    
    adata = {}
    for j, avalue in actors.iteritems():
        #log("INFL Actors Dict: " + str(avalue.__dict__))
        #log("INFL Actors Instance Dict: " + str(avalue.instance.__dict__))
        #log("INFL Actors J, Value: " + str(j) + " " + str(avalue.instance.id))
        adata[j] = {
            'id': avalue.instance.id,
            'name': avalue.name,
            'state': avalue.state
        }
        if avalue.state == 0:
            adata[j]['state'] = False
        else:
            adata[j]['state'] = True
        #log("INFL Actors Data: " + str(adata[j].get('id')))
    #log("INFL Actors AData: " + str(adata))   
        
    fdata = {}
    for k, fvalue in fermenter.iteritems():
        #log("INFL Fermenter Dict: " + str(fvalue.__dict__))
        fdata[k] = {
            'id': fvalue.id,                ##Fermenter ID
            'name': fvalue.name,            ##Fermenter Name
            'target': fvalue.target_temp,   ##Fermenter TargetTemp
            'heater': fvalue.heater,        ##Fermenter Heater ID
            'cooler': fvalue.cooler,        ##Fermenter Cooler ID
            'sensor1': fvalue.sensor,       ##Fermenter Sensor1 ID
            'sensor2': fvalue.sensor2,      ##Fermenter Sensor2 ID
            'sensor3': fvalue.sensor3,      ##Fermenter Sensor3 ID
            'status': fvalue.state          ##Fermenter Automatic State 0 or 1
        }
        #log("INFL Fermenter Data: " + str(fdata[k]))
    #log("INFL Fermenter FData: " + str(fdata))
    
    for _ in range(len(fdata)):
        if len(ferm_target_temp_old) < len(fdata):
            ferm_target_temp_old.append(None)
        if len(ferm_auto_state_old) < len(fdata):
            ferm_auto_state_old.append(None)
        if len(act_cool_state_old) < len(fdata):
            act_cool_state_old.append(None)
        if len(act_heat_state_old) < len(fdata):
            act_heat_state_old.append(None)
        if len(sen_1_temp_old) < len(fdata):
            sen_1_temp_old.append(None)
        if len(sen_2_temp_old) < len(fdata):
            sen_2_temp_old.append(None)
        if len(sen_3_temp_old) < len(fdata):
            sen_3_temp_old.append(None)
    
    #log("ferm_target_temp_old: " + str(len(ferm_target_temp_old)))
    #log("INFL Fermenter FData Items: " + str(fdata.iteritems().__dict__))
    json_payload = []
    for idx, data in fdata.items():
        #log("")
        #log("INFL Fermenter Data Items: " + str(fdata[idx]))
        #log("Index " + str(idx))
        
        idn = idx - 1
        
        ferm_target_temp = fdata[idx].get('target')
        #log("ferm_target_temp: " + str(ferm_target_temp) + " old: " + str(ferm_target_temp_old[idn]))
        if ferm_target_temp != ferm_target_temp_old[idn]:
            ferm_target_temp_old[idn] = ferm_target_temp
            isChanged = True
        
        ferm_auto_state = fdata[idx].get('status')
        #log("ferm_auto_state: " + str(ferm_auto_state) + " old: " + str(ferm_auto_state_old[idn]))
        if ferm_auto_state != ferm_auto_state_old[idn]:
            ferm_auto_state_old[idn] = ferm_auto_state
            isChanged = True        
        
        has_cooler = fdata[idx].get('cooler')
        #log("has_cooler: " + str(has_cooler))
        if has_cooler != '':
            coolerid = int(has_cooler)
            act_cool_name = adata[coolerid].get('name')
            act_cool_state = adata[coolerid].get('state')
            #log("act_cool_state: " + str(act_cool_state) + " old: " + str(act_cool_state_old[idn]))
            if act_cool_state != act_cool_state_old[idn]:
                act_cool_state_old[idn] = act_cool_state
                isChanged = True
        else:
            act_cool_name = ''
            act_cool_state = False
            act_cool_state_old[idn] = act_cool_state
        #log("type act_cool_state: " + str(type(act_cool_state)))
        
        has_heater = fdata[idx].get('heater')
        #log("has_heater: " + str(has_heater))
        if has_heater != '':
            heaterid = int(has_heater)
            act_heat_name = adata[heaterid].get('name')
            act_heat_state = adata[heaterid].get('state')
            #log("act_heat_state: " + str(act_heat_state) + " old: " + str(act_heat_state_old[idn]))
            if act_heat_state != act_heat_state_old[idn]:
                act_heat_state_old[idn] = act_heat_state
                isChanged = True
        else:
            act_heat_name = ''
            act_heat_state = False
            act_heat_state_old[idn] = act_heat_state
        #log("type act_heat_state: " + str(type(act_heat_state)))
        
        has_sensor1 = fdata[idx].get('sensor1')
        #log("has_sensor1: " + str(has_sensor1))
        if has_sensor1 != '':
            sensorid = int(has_sensor1)
            sen_1_name = sdata[sensorid].get('name')
            sen_1_temp = sdata[sensorid].get('value')
            #log("sen_1_temp: " + str(sen_1_temp) + " old: " + str(sen_1_temp_old[idn]))
            if sen_1_temp_old[idn] != sen_1_temp:
                sen_1_temp_old[idn] = sen_1_temp
                isChanged = True
        else:
            sen_1_name = None
            sen_1_temp = None
            sen_1_temp_old[idn] = sen_1_temp
        #log("type sen_1_temp: " + str(type(sen_1_temp)))
        
        has_sensor2 = fdata[idx].get('sensor2')
        #log("has_sensor2: " + str(has_sensor2))
        if has_sensor2 != '':
            sensorid = int(has_sensor2)
            sen_2_name = sdata[sensorid].get('name')
            sen_2_temp = sdata[sensorid].get('value')
            #log("sen_2_temp: " + str(sen_2_temp) + " old: " + str(sen_2_temp_old[idn]))
            if sen_2_temp_old[idn] != sen_2_temp:
                sen_2_temp_old[idn] = sen_2_temp
                isChanged = True
        else:
            sen_2_name = None
            sen_2_temp = None
            sen_2_temp_old[idn] = sen_2_temp
        #log("type sen_2_temp: " + str(type(sen_2_temp)))
        
        has_sensor3 = fdata[idx].get('sensor3')
        #log("has_sensor3: " + str(has_sensor3))
        if has_sensor3 != '':
            sensorid = int(has_sensor3)
            sen_3_name = sdata[sensorid].get('name')
            sen_3_temp = sdata[sensorid].get('value')
            #log("sen_3_temp: " + str(sen_3_temp) + " old: " + str(sen_3_temp_old[idn]))
            if sen_3_temp_old[idn] != sen_3_temp:
                sen_3_temp_old[idn] = sen_3_temp
                isChanged = True
        else:
            sen_3_name = None
            sen_3_temp = None
            sen_3_temp_old[idn] = sen_3_temp
        #log("type sen_3_temp: " + str(type(sen_3_temp)))
        
        fermdata = {
            "measurement": "CBPiChiller",
            "tags": {
                "ferm_name": str(fdata[idx].get('name'))
                #"ferm_id": str(data[key].get('id'))
                },
            "fields": {
                'ferm_target_temp': ferm_target_temp,
                'ferm_auto_state': ferm_auto_state,
                'act_cool_name': str(act_cool_name),
                'act_cool_state': act_cool_state,
                'act_heat_name': str(act_heat_name),
                'act_heat_state': act_heat_state,
                'sen_1_name': str(sen_1_name),
                'sen_1_temp': sen_1_temp,
                'sen_2_name': str(sen_2_name),
                'sen_2_temp': sen_2_temp,
                'sen_3_name': str(sen_3_name),
                'sen_3_temp': sen_3_temp
            }
        }
        
        json_payload.append(fermdata)
    
    #log("INFL Payload: " + str(json_payload))
    if isChanged == True:
        #Send payload
        log("Send Data to DB....")
        isSended = client.write_points(json_payload)
        log("INFL Sended to DB >>>>>>>>>>>>>>>>>>>>>>>>> " + str(isSended))
        isChanged = False
    else:
        log("No changes right !!!!!!!!!!!!!!!!!!!!!!!!")
    client.close()


    

