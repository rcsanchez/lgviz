#!/usr/bin/python

import psycopg2

dbname="logvisualizer"
user="rcrescen"
host="localhost"
password="rcrescen"

try:

    conn=psycopg2.connect("dbname='"+dbname+"' user='"+user+"' host='"+host+"' password='"+password+"'")
    cur=conn.cursor()

    cur.execute("""SELECT request_url_c from nodes order by id""")
    nodes=cur.fetchall()

    cur.execute("""SELECT session_id from sessions order by id""")
    sessions=cur.fetchall();

    for index in range(0, len(sessions)):
        session=sessions[index][0]
        cur.execute("""SELECT request_url_c from tracker_c where session_id='"""+session+"""' order by id""")
        path=cur.fetchall()
        if (len(path)>1):
            for idx in range(1, len(path)):
                cur.execute("""INSERT INTO paths(session_id,start, finish) VALUES (%s,%s,%s)""", (session, str(path[idx-1][0]), str(path[idx][0])))
        else:
            cur.execute("""INSERT INTO paths(session_id,start) VALUES ('"""+session+"""', '"""+str(path[0][0])+"""')""")

        if(index%100==0):
            print str(index)+"-"+str(len(sessions))+" as "+session
            conn.commit()
        if(index==len(sessions)-1):
            conn.commit()
        

except:
    print "Unable to connect with "+dbname;
