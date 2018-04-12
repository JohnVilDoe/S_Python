#!/usr/bin/env python
"""
Created by   : John van Vilsteren
Purpose      : Change bankaccount(s) based on XML-file(s) from sFTP server
Version date : 20180111
Version upd  : Changes to run on the server
--Process was based on XLS files and this has changed because we receive XML files form ING now
"""

import sys
# add the path to the modules otherwise some won't work :-(
#sys.path.append('/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages')

import xml.etree.ElementTree as ET
import csv
import datetime
import psycopg2
import urllib2
import json
import os
import errno
import shutil
import paramiko
from stat import S_ISDIR
import time
import logging
import logging.handlers

log = logging.getLogger()

FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"
script_dir = os.path.dirname(os.path.realpath(__file__))
log_dir = os.path.join(script_dir, 'log')
try:
    os.mkdir(log_dir)
except:
    pass
log_file = os.path.join(log_dir, os.path.basename(
    __file__).split('.')[0] + ".log")
fh = logging.handlers.RotatingFileHandler(
    log_file, maxBytes=1024 * 1024, backupCount=10)
fh.setFormatter(logging.Formatter(FORMAT))
log.addHandler(fh)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter(FORMAT))
log.addHandler(ch)

class Gdict(dict):
    """A subclass of dict that additionally allows attribute access to its values."""
    __setattr__ = dict.__setitem__
    __getattr__ = dict.__getitem__

    def to_json(self):
        """returns the json string representation of the argument"""
        return json.dumps(self, indent=4)

    def __str__(self):
        return self.to_json()


def json2gdict(j):
    """Returns the input with all dict objects recursively replaced by Gdict objects"""
    if type(j) in (list, tuple):
        return type(j)(json2gdict(elt) for elt in j)
    elif type(j) in (dict, Gdict):
        return Gdict((k, json2gdict(v)) for k, v in j.items())
    return j


settings = Gdict()
connection = None
cursor = None

def read_settings(input_file=None):
    if not input_file:
        current_dir = os.path.dirname(os.path.realpath(__file__))
        input_file = os.path.join(current_dir, 'settings.json')
    with open(input_file) as o:
        res = json2gdict(json.load(o))
    settings.update(res)
    if 'db' in settings and 'date_fields' in settings.db:
        for date_field in settings.db.date_fields.split(','):
            date_fields.append(date_field.strip().lower())

def init_db():
    global connection
    connection = psycopg2.connect(settings.db.conn_string)
    log.info("connected to %s", connection.dsn)
    global cursor
    cursor = connection.cursor()


def close_db():
    cursor.close()
    connection.close()
    log.info("db connection closed")


# Function to make connection to the FTP site
def init_ftp():
    global sftp
    # setting up the connection
    transport = paramiko.Transport((settings.ftp.host, settings.ftp.port))
    transport.connect(username = settings.ftp.username, password = settings.ftp.password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    log.info('Connected to %s',  settings.ftp.host)

# Function to change the path on the sFTP server
def change_dir_ftp():
    sftp.chdir(settings.rPath.output)

# Function to loop through the folders on the sFTP site
def sftp_walk():
    path=settings.rPath.ovs
    for f in sftp.listdir_attr(path):
        if not S_ISDIR(f.st_mode):
            yield path, f.filename

# Function to get files from a FTP server
def get_files_from_ftp(rows):
    for path, file in rows:
        sftp.get(path + file, sys.path[0]+settings.lPath.input + file)
        log.info('File : ' + file + ' Downloaded from the sFTP server')
        yield file

# Function to move/rename a file on a FTP server
def move_file_ftp(files):
    for file in files:
        sftp.rename(settings.rPath.ovs + file, settings.rPath.download + file)
        log.info('File :' + file + ' moved to the downloaded folder.')

# Function to move/rename a local file
def move_local_file(file):
    try:
        shutil.move(file, file.replace(settings.lPath.input, settings.lPath.prcssd))
        return True
    except Exception as e:
        log.error(str(e))
        return False

def close_ftp():
    sftp.close()
    log.info("sFTP connection closed")

def path_walk():
    lPath = sys.path[0] + settings.lPath.input
    # Loop through the local folder
    for fn in os.listdir(lPath):
        if os.path.isfile(os.path.join(lPath, fn)):
            yield os.path.join(lPath, fn)

# Function to check if the dir exists
def make_sure_dir_exists(sPath):
    if not os.path.exists(sPath):
        os.makedirs(sPath)

def Chk_lPaths():
    # Check if the used folders exists
    log.info('Check if the used local folder(s) defined in the settings file does exist')
    for p in settings.lPath:
        log.info('Path x created : ' +  sys.path[0] + settings.lPath[p])
        make_sure_dir_exists(sys.path[0] + settings.lPath[p])

def processfile(fileList):

    for file in fileList:

        log.info('Start to process the file : ' + file)

        # Var for the number of records to process and to check if there are really that mnumber of upodates
        NbRec = []

        # Put the XML (string) into a var
        root = ET.parse(str(file))

        ## We need to get the number of entries:
        strWhatToFind =  prefix + 'IngOvrstpsrvcRpt/' + prefix + 'Rpt/' + prefix + 'TxsSummry/' + prefix + 'TtlNtries/' + prefix + 'NbOfNtries'
        NbOfNtries = int(root.find(strWhatToFind).text)

        if NbOfNtries:
            log.info('Found NbOfNtries : ' + str(NbOfNtries))

        # Build the list with rows in order to process them later
        rows= []
        for ntry in root.getiterator(prefix+'Ntry'):
            a_list = list(ntry.getiterator())

            row = []

            for el in a_list:

                strElTag = str.replace(el.tag, prefix, '')
                strElText = str(el.text).strip()

                if strElTag == 'MndtId':
                    row.append(strElText)
                elif strElTag == 'EndToEndId':
                    row.append(strElText)
                elif strElTag == 'IBAN':
                    row.append(strElText)
                elif strElTag == 'BIC':
                    row.append(strElText)

            rows.append(row)

            log.info('Check if the number of records is equal to the number of entries in the file')
            if len(rows) == NbOfNtries:
                log.info('File is okay so we can process this file')

                for row in rows:
                    body = ret_body(row[0], row[2], row[3], row[4], row[5])
                    if body is None:
                        log.info('We skip this, there is no customer with the following mandate reference : ' + str(row[0]))
                    else:
                        try:
                            resp = call_rpc("CreateIBAN", body)
                            # Add history line
                            ins_hist = "Insert into crm.history (customer_id, created_at, created_by, type, message) values (" + str(rec[0]) + ", now(), 'Operations', 'Finance', 'Rekeningnummer is op " + datetime.datetime.strftime(datetime.datetime.now(),'%d-%m-%Y') + " door Operations aangepast')"
                            cursor.execute(ins_hist)
                            connection.commit()
                        except:
                            print "could not change IBAN: {0}".format(sys.exc_info()[1])
                            continue

                if not move_local_file(file):
                    log.error('Moving a local file to the processed folder failed')
            else:
                log.error('The number of entries given and the number of real entries are not equal')


# Return the body for the call to the RPC call
def ret_body(mandate_ref, iban_old, bic_old, iban_new, bic_new):
    sel_sql  = "select customer_id, owner_name from crm.customer "
    sel_sql += " where mandate_reference = '" + mandate_ref
    sel_sql += "' and iban = '" + iban_old
    sel_sql += "' and status = 'ACTIVE' "
    sel_sql += " and ended_at IS NULL order by id desc limit 1"

    cursor.execute(sel_sql)
    for rec in cursor:
		print 'Customer_id : '  + str(rec[0])
		return {'CustomerID': str(rec[0]), 'IBAN': iban_new, 'BIC': bic_new, 'OwnerName': rec[1]}


# Function to call a RPC-Call
def call_rpc(method, body):

    print 'End_point : ', settings.end_point.value

    req = json.dumps({
        "method": "RPC." + method,
        "params": [
            body
        ],
        "id": 0
    })
    resp = json.load(urllib2.urlopen(settings.end_point.value, req))
    if resp["error"]:
        raise Exception(
            "error calling {0} with param {1}: {2}".format(method, body, resp["error"]))
    return resp

# function to 
def ins_db_rec(sSQL):
    try:
        curIns = conn.cursor()
        curIns.execute(sSQL)
        conn.commit()
        curIns.close()
        return True
    except Exception as e:
        log.error(str(e))
        return False

# Start time
start = time.clock()

# Start of the process
log.info('***********************************************************')
log.info('*              Start with Change bankaccount              *')
log.info('*                   ' + datetime.datetime.strftime(datetime.datetime.now(),'%d-%m-%Y %H:%M:%S') + '                   *')
log.info('***********************************************************')

# Vars used
yearweek = datetime.datetime.strftime(datetime.datetime.now(),'%Y%U')
settings = Gdict()
connection = None
cursor = None
prefix = '{http://www.ing.com/}'
header = ('mndtid', 'endtoendid','iban_old','bic_old','iban_new','bic_new')
fileListAll = []



# This is the start of the program ;-)
if __name__ == '__main__':
    try:
        # Reed the settings
        read_settings()
        # Set log level
        log.setLevel(getattr(logging, settings.loglevel.upper()))
        # Check if local paths are there, else create them
        Chk_lPaths()
        # Open the (remote) DB
        init_ftp()
        # Open the sFTP
        init_db()
        # Get the files from the sFTP server
        move_file_ftp(get_files_from_ftp(sftp_walk()))
        # Process the files from the server if there are
        processfile(path_walk())

        close_db()
        close_ftp()

    except Exception , e:
        log.error(str(e))


