#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Extract data-dumps (XML-backups) from 1Cfresh.com cloud

# Use standard libs - no external dependencies !!
import urllib.request
import json
import pathlib

# User-defined variables
username=""
password=""

oneassfresh_accound_id=

server='https://1cfresh.com'

backups_directory = '/volume2/1C_Fresh//'

#print ('1C-Fresh Backup script')
#print ('   Server: '+server)
#print ('   Login: '+username)


# HTTP-401-Auth support
password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()   # Create password manager
password_mgr.add_password(None, server, username, password)       # Add login/pass to server in pwd-manager
opener = urllib.request.build_opener(urllib.request.HTTPBasicAuthHandler(password_mgr)) # object for authorized http req

urllib.request.install_opener(opener)     # Set as default opener for all urllib.request !!


# Get json with list of tenants (database objects)
url_tlist = server+'/a/adm/hs/ext_api/execute'
post_data = '{ "general": { "type": "usr", "method": "tenant/list", "debug": true }, \
     "auth": {"account": ' + str(oneassfresh_accound_id) + '}}'

tlist_obj = urllib.request.urlopen(url_tlist, post_data.encode('utf-8'))
tlist_str = tlist_obj.read().decode('utf-8')     # req - read - decode
#print (tlist_str)

# Parse JSON
tjson_obj = json.loads(tlist_str)
tenants = tjson_obj['tenant']

dbs = {}   # Database properties by iD

delList = []

for tenant in tenants:
    if (tenant['status'] != "used"): 
        delList.append(tenant['id']) 
        continue  # Skip unused databases

    tid = tenant['id']
    #print ('   ** БД: iD: ' + str(tid) + \
    #           ' Version: ' + tenant['app_name'] + ' ' + tenant['app_version'] + \
    #              ' Name: '+ tenant['name'])
    dbs.setdefault(tid, {})
    dbs[tid]['ts'] = '1970-01-01T00:00:01'
    dbs[tid]['name'] = tenant['name']
    dbs[tid]['ver'] = tenant['app_version']
    dbs[tid]['app_name'] = tenant['app_name']
    dbs[tid]['dir'] = pathlib.Path(backups_directory, tenant['name'])
    # Create subdirectories for databases
    if not pathlib.Path.is_dir(dbs[tid]['dir']):
        try: pathlib.Path.mkdir(dbs[tid]['dir']) #Create dir if not exist
        except OSError: dbs[tid]['dir'] = ''
#print(dbs)

# Get json with list of backups
url_blist = server+'/a/adm/hs/ext_api/execute'
post_data = '{ "general": { "type": "usr", "method": "backup/list", "debug": false }, \
     "auth": {"account": ' + str(oneassfresh_accound_id) + '}}'

blist_obj = urllib.request.urlopen(url_blist, post_data.encode('utf-8'))
blist_str = blist_obj.read().decode('utf-8')     # req - read - decode
#print (blist_str)

# Parse JSON
bjson_obj = json.loads(blist_str)
backups = bjson_obj['backup']

for b in backups:
    if (b['tenant'] in delList): continue
    tid = b['tenant']
    ts = b['timestamp']
    #print ('   ** Backup: UUiD: ' + str(b['id']) + ' App iD: ' + str(tid) + ' TS: ' + ts + ' Name: '+ dbs[tid]['name'] + ' Version: ' + b['app_version'])
    if (ts > dbs[tid]['ts']):
       dbs[tid]['ts'] = ts
       dbs[tid]['uuid'] = b['id']
       dbs[tid]['ver'] = b['app_version']
#print(dbs)

# Prepare download
url_dlist = server+'/a/adm/hs/ext_api/execute'
for dl in dbs:
    if (dbs[dl]['dir'] == ''): continue  # Skip databases without paths
    uuid = dbs[dl]['uuid']
    fname = pathlib.Path(dbs[dl]['dir'], dbs[dl]['name'] + '_' + str.replace(dbs[dl]['ts'], ':', '-') + '_' + dbs[dl]['ver'] + '.zip')
    if pathlib.Path.is_file(fname): 
        print('   ** Skip file: ' + fname._str)
        continue # Skip, if file exist
    post_data = '{ "id": "' + uuid + '", "general": { "version": 9, "type": "usr", \
       "method": "backup/file_token/download", "debug": true }, \
         "auth": {"account": ' + str(oneassfresh_accound_id) + ', "type": "user" }}'
    # get download token for each most fresh backup
    dlist_obj = urllib.request.urlopen(url_dlist, post_data.encode('utf-8'))
    dlist_str = dlist_obj.read().decode('utf-8')     # req - read - decode
    # Parse JSON
    djson_obj = json.loads(dlist_str)
    token = djson_obj['token']
    url = djson_obj['url']
    error = djson_obj['general']['error']
    response = djson_obj['general']['response']
    print ('   ** Backup: UUiD: ' + str(uuid) + ' Database name: ' + dbs[dl]['name'])
    #print ('      Token: ' + str(token))
    if (error != False):
        print(dlist_str)
    else:
        local_filename, headers = urllib.request.urlretrieve(url, filename=fname)
        #print(headers) 


