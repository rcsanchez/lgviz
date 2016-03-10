#!/usr/bin/python

import psycopg2
import json
import pprint

dbname="logvisualizer"
user="rcrescen"
host="localhost"
password="rcrescen"

try:

    conn=psycopg2.connect("dbname='"+dbname+"' user='"+user+"' host='"+host+"' password='"+password+"'")
    cur=conn.cursor()

    #cur.execute("""SELECT count, start, finish from graph""")
    #edges=cur.fetchall()
    #links=[]
    #for index in range(0, len(edges)):
    #    links.append({'source':str(edges[index][1]), 'target':str(edges[index][2]), 'value':"{:.8f}".format(edges[index][0]/199680.0)})
    #pprint.pprint(links)

    cur.execute("""SELECT request_url_c from nodes""")
    urls=cur.fetchall()
    nodes=[]
    for index in range(0, len(urls)):
        nodes.append({'name':str(urls[index][0])})

    pprint.pprint(nodes)

except:
    print "Unable to connect with "+dbname;
