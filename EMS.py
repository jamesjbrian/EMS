from smbus2 import SMBus
from bme280 import BME280
try:
    # Transitional fix for breaking change in LTR559
    from ltr559 import LTR559
    ltr559 = LTR559()
except ImportError:
    import ltr559
    
import time
import datetime
import threading
import pandas as pd
import sqlite3

from enviroplus import gas

from pms5003 import PMS5003

pms5003 = PMS5003()

bus = SMBus(1)

dvc = BME280(i2c_dev=bus)

from EMSConfiguration import *


b = 1
c = 0
#conn = sqlite3.connect(dbloc)
#cursor = conn.cursor()

print('let it begin...')

def dbcommit(htpc,ligc,gasc,pmsc):
    conn = sqlite3.connect(dbloc)
    cursor = conn.cursor()
    cursor.execute("insert into HTP values (?,?,?,?,?)", htpc)
    cursor.execute("insert into Light values (?,?,?,?)", ligc)
    cursor.execute("insert into Gas values (?,?,?,?,?)", gasc)
    cursor.execute("insert into Particulates values (?,?,?,?,?,?,?,?,?,?,?)", pmsc)
    conn.commit()
    

def analysis(htpl,ligl, gasl, pmsl):
    print('in a thread')
    htpdf = pd.DataFrame(htpl, columns=['DeviceID', 'Time','Humidity','Temperature','Pressure'])
    ligdf = pd.DataFrame(ligl, columns=['DeviceID', 'Time','Lux','Proximity'])
    gasdf = pd.DataFrame(gasl, columns=['DeviceID', 'Time', 'Reducing','Oxidising','NH3'])
    pmsdf = pd.DataFrame(pmsl, columns=['DeviceID', 'Time', 'PM1','PM2.5','PM10','Air0.3','Air0.5','Air1','Air2.5','Air5','Air10'])

    htpc = (DeviceID, str(datetime.datetime.now()), htpdf.loc[:,'Humidity'].mean(),htpdf.loc[:,'Temperature'].mean(),htpdf.loc[:,'Pressure'].mean())
    ligc = (DeviceID, str(datetime.datetime.now()), ligdf.loc[:,'Lux'].mean(),ligdf.loc[:,'Proximity'].mean())
    gasc = (DeviceID, str(datetime.datetime.now()), gasdf.loc[:,'Reducing'].mean(),gasdf.loc[:,'Oxidising'].mean(),gasdf.loc[:,'NH3'].mean())
    pmsc = (DeviceID, str(datetime.datetime.now()), pmsdf.loc[:,'PM1'].mean(),pmsdf.loc[:,'PM2.5'].mean(),pmsdf.loc[:,'PM10'].mean(),pmsdf.loc[:,'Air0.3'].mean(),pmsdf.loc[:,'Air0.5'].mean(),pmsdf.loc[:,'Air1'].mean(),pmsdf.loc[:,'Air2.5'].mean(),pmsdf.loc[:,'Air5'].mean(),pmsdf.loc[:,'Air10'].mean())
    
    print(htpc)
    print(ligc)
    print(gasc)
    print(pmsc)
    
    dbcommit(htpc,ligc,gasc,pmsc)
   

def scan(inc):
    global b, c
    htpl = []
    ligl = []
    gasl = []
    pmsl = []
    a = 1
    
    while a <= inc:
        b = b + 1
        print('a:',a,'b:',b, 'c:',c)
        readings = gas.read_all()
        pms = pms5003.read()
        dvc.get_humidity(), dvc.get_temperature(),  dvc.get_pressure()
        ltr559.get_lux(), ltr559.get_proximity()
        
        if b >= waittime:
            c = 1
            htpl.append((DeviceID, str(datetime.datetime.now()), dvc.get_humidity(), dvc.get_temperature(),  dvc.get_pressure()))
            ligl.append((DeviceID, str(datetime.datetime.now()), ltr559.get_lux(), ltr559.get_proximity()))
            gasl.append((DeviceID, str(datetime.datetime.now()), readings.reducing, readings.oxidising, readings.nh3))
            pmsl.append((DeviceID, str(datetime.datetime.now()), pms.pm_ug_per_m3(1), pms.pm_ug_per_m3(2.5), pms.pm_ug_per_m3(10), pms.pm_per_1l_air(0.3),pms.pm_per_1l_air(0.5),pms.pm_per_1l_air(1.0),pms.pm_per_1l_air(2.5),pms.pm_per_1l_air(5),pms.pm_per_1l_air(10)))
            a = a + 1
        
        time.sleep(sleeptime)
        
    if c == 1:
        print('starting thread')
        t = threading.Thread(target=analysis, args=(htpl,ligl,gasl,pmsl))
        t.start()
        
    htpl = []
    ligl = []
    gasl = []
    pmsl = []
    a = 0
            
while True:
    scan(inc)
    
