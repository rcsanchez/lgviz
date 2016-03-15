#!/usr/bin/python

import psycopg2

dbname="logvisualizer"
user="rcrescen"
host="localhost"
password="rcrescen"

backbones=["/biodiversidad/",\
           "/curacion/biodiversidad/",\
           "/obraartistica/",\
           "/curacion/obraartistica/",\
           "/proyectosuniversitarios/",\
           "/proyectosuniversitarios/"]
users_path={}

try:
    
    conn=psycopg2.connect("dbname='"+dbname+"' user='"+user+"' host='"+host+"' password='"+password+"'")
    cur=conn.cursor()

    print 'fetching from DB...'
    cur.execute("""SELECT session_id,referer, request_url from log_a order by folio""")
    nodes=cur.fetchall()
    nodes_length=len(nodes)
    print 'extract sessions...'
    for index in range(0, len(nodes)):
        if(index%100000==0):
            print (index*100)/nodes_length
        session=str(nodes[index][0])
        if(session in users_path):
            users_path[session]['nav_path'].append(str(nodes[index][1]))
        else:
            users_path[session]={'nav_path':[str(nodes[index][1])]}
    print 'sessions extracted successfull'
    print ''len(users_path)
        

except psycopg2.Error as e:
    print e.pgerror;
