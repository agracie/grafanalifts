#!/usr/bin/python3

import gspread
from influxdb import InfluxDBClient
from oauth2client.service_account import ServiceAccountCredentials
import os
import re

# Creds
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('<SPREADSHEET_JSON_CRED_FILE>', scope)
client = gspread.authorize(creds)
spreadsheet = client.open("<SPREADSHEET_NAME>")

def influx_updatedb(datalist, title):
    """
    Connects to the DB, drops the BD and writes a fresh one for a worksheet
    """
    dbclient = InfluxDBClient(host='localhost', port=8086)
    dbclient.drop_database(title)
    dbclient.create_database(title)
    dbclient.write_points(datalist,None,title,protocol='json')

def get_worksheet_title():
    """
    Gets names of worksheets on spreadsheet
    """
    wkshts = spreadsheet.worksheets()
    titles = []
    for wksht in wkshts:
      titles.append(wksht.title)
    return titles

def get_worksheet_data(title):
    """
    Gets cells in a given workseet
    """
    title = spreadsheet.worksheet(title)
    return title.get_all_records()

# Begin

for title in get_worksheet_title():
  newdata = get_worksheet_data(title)
  datalist = []
  for data in newdata:
      time = data["time"]
      data.pop("time")
      lift_dict = { "measurement" : "Lifts",
                   "fields" : data,
                   "time" : time }

      datalist.append(lift_dict)
  influx_updatedb(datalist, title)
