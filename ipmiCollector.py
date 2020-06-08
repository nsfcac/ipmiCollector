import socket
import json
#import datetime
from datetime import datetime
import subprocess
import collections
import warnings
import re
from threading import Thread
import multiprocessing

from influxdb import InfluxDBClient

def get_temp_power(ip):
    
    # To See Temperature and power Sensors, Use the following command, with the full argument:
    
    outputData = {}
    
    try:
        sensors_data = subprocess.check_output('ipmitool -I lanplus -H ' + ip + ' -U ' + userName + ' -P ' + passwd +' sdr elist full | egrep -v "Disabled|No Reading"', shell=True).decode('ascii')
        #print (sensors_data)
    except subprocess.CalledProcessError as e:
        #raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
        return None,e.output
    

    lines = sensors_data.split('\n')

    for line in lines:
        if line:
            fields = line.split('|')
            k = fields[0].replace(" ","")
            if k in outputData.keys():
                outputData[k+'1'] = outputData[k]
                del outputData[k]
                k = k+'2'
            outputData[k] = fields[len(fields)-1].split()[0].replace(" ","")
                      #outputData[fields[0].replace(" ","")] = fields[len(fields)-1].split()[0].replace(" ","") 
    
    return outputData, None    


def get_power_status(ip):

    try:
        response = subprocess.check_output('ipmitool -I lanplus -H ' + ip + ' -U ' + userName + ' -P ' + passwd + ' power status', shell=True).decode('ascii')
        if (response.rfind("on") == -1):
            return 'Off',None
        else:
            return 'On',None
    except subprocess.CalledProcessError as e:
        return None,e.output

def getNodesData (host_name, ip, json_node_list, error_list):
    
    # Get the temperature and power metrics of the host
    hostIPMIData = {}

    hostThermalPwrUsage, error = get_temp_power(ip)
    if hostThermalPwrUsage != None:
        hostIPMIData = hostThermalPwrUsage
    
    # Get the power statu: On or Off
    hostPwrState, error = get_power_status(ip)
    
    # Encapsulate IPMI monitoring data into a python dictionary:
    if hostPwrState != None:
        hostIPMIData['PowerState'] = hostPwrState
        hostIPMIData['node'] = host_name

    # Convert Python dictionary into JSON data
    if bool(hostIPMIData):
        #json_data =json.dumps(hostIPMIData)
        json_node_list.append(hostIPMIData)
        error_list.append([host_name, ip, error])

def proc_to_threads (input_data):
    
    warnings.filterwarnings('ignore', '.*', UserWarning,'warnings_filtering',)
    try:
        #print (input_data)
        error_list = []
        json_node_list = []
        threads = []   
        thread_id = 0
        for host_info in input_data:
            host_name = host_info[0]
            ip = host_info[1]
            a = Thread(target = getNodesData, args=(host_name, ip, json_node_list, error_list, ))
            threads.append(a)
            threads[thread_id].start()
            thread_id += 1

        for index in range (0, thread_id):
            threads[index].join()

            

        return json_node_list, error_list

    except Exception as e:
        error_list.append([ip, e])
        return json_node_list, error_list
    
def getNodeData (input_data):
    
    warnings.filterwarnings('ignore', '.*', UserWarning,'warnings_filtering',)
    try:
        nodes = len(input_data)
        if (nodes == 0):
            print ("\nThere is no node for monitoring!\n")
            return [], []
        
        node_error_list = []
        node_json_list = []
        
        #########################################################################
        # Initialize pool
        cores = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(cores)

        if ( nodes < cores ):
            cores = nodes

        print("Monitoring %d hosts using %d cores..." % \
            	(nodes, cores) )
        
        # Build job list
        jobs = []
        
        hosts_per_proc = nodes // cores
        surplus_hosts = nodes % cores

        
        #print (input_data)
        for p in range (cores):
            if (surplus_hosts != 0 and p == (cores-1)):
                jobs.append( (input_data[p*hosts_per_proc:p*hosts_per_proc+hosts_per_proc-1] + input_data[p*hosts_per_proc+hosts_per_proc-1:p*hosts_per_proc+hosts_per_proc] + input_data[p*hosts_per_proc+hosts_per_proc:p*hosts_per_proc+hosts_per_proc+surplus_hosts] ,))
            else:
                jobs.append( (input_data[p*hosts_per_proc:p*hosts_per_proc+hosts_per_proc-1] + input_data[p*hosts_per_proc+hosts_per_proc-1:p*hosts_per_proc+hosts_per_proc],))                  
            #print (input_data[proc*threads_per_proc:proc*proc*threads_per_proc+threads_per_proc-1+surplus_threads])
                              
        # Run parallel jobs
        results = [pool.apply_async( proc_to_threads, j ) for j in jobs]

        # Process results
        for result in results:
            (node_data, node_error) = result.get()
            node_json_list += node_data
            node_error_list += node_error
        pool.close()
        pool.join()

        #########################################################################         

        return node_json_list, node_error_list
    except Exception as e:
        node_error_list.append([e])
        return node_json_list, node_error_list

def build_ipmi_metrics(hostData,ts):
    
    hostMetList = []
    
    for key,val in hostData.items():
        if key == 'node':
            continue
        hostMetList.append({'measurement':'IPMI','tags':{'Label':key,'NodeId':hostData['node']},'time':ts,'fields':{'Reading':val}} )
    return hostMetList
        

def store_ipmi_mets(ipmiMetricsList):
    
    # storing results in InfluxDBClient                                                                                                                     
    client = InfluxDBClient(host='10.101.92.201', port=8086)
    client.switch_database('monitoring_HW_Sch_OS')
    client.write_points(ipmiMetricsList,time_precision='s')
    

userName = ""
passwd = ""

def main():
    
    ts = datetime.now().isoformat()
    #ts = int(datetime.now().timestamp())

     # Read BMC Credentials:
    with open('/home/ghali/bmc_cred.txt','r') as bmc_cred:
        bmcCred = json.load(bmc_cred)
        
    global userName
    global passwd

    userName = bmcCred[0]
    passwd = bmcCred[1]

    hostsIP = []
    hostsID = []
    input_data = []
    
    input_data1 = []
    
    for i in range (31):
        postfix = str(i+1)
        hostsIP.append('10.101.91.'+postfix)
        hostsID.append('zc-91-'+postfix)
        
    for i in range (33):
        postfix = str(i+1)
        hostsIP.append('10.101.92.'+postfix)
        hostsID.append('zc-92-'+postfix)
    
    for id,ip in zip(hostsID,hostsIP):
        input_data.append([id, ip])
        
    
    nodes_data, error_list =  getNodeData(input_data)

    ipmiMetricsList = []

    for nodeData in nodes_data:
        print ('\n\n********************************Host: ',nodeData["node"],'***************************************')
        print(nodeData)
        hostMets = build_ipmi_metrics(nodeData,ts)
        ipmiMetricsList += hostMets
    
    print ('\n\nTotal responding nodes: ',len(nodes_data),'\tTotal data points (InfluxDB):', len(ipmiMetricsList))
    
    #print (ipmiMetricsList)
    store_ipmi_mets(ipmiMetricsList)
   
if __name__== "__main__":
  main()
