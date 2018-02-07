#!/usr/bin/python

from urlparse import urlparse
import psycopg2
import pprint
import networkx as nx
import matplotlib.pyplot as plt

def clear_ficha(ficha):
    ficha_flat=ficha
    if(ficha.count(':')>0):
        if('curacion' not in ficha):
            ficha_flat='/ficha'
        else:
            ficha_flat='/curacion/ficha'
    return ficha_flat


dbname="http_log"
user="reynaldo"
host="10.1.6.58"
password="foo"
pretty_relations={}

backbones=["/biodiversidad/",\
           "/curacion/biodiversidad/",\
           "/obraartistica/",\
           "/curacion/obraartistica/",\
           "/proyectosuniversitarios/",\
           "/curacion/proyectosuniversitarios/"]
users_path={}
relations={}

try:
    
    conn=psycopg2.connect("dbname='"+dbname+"' user='"+user+"' host='"+host+"' password='"+password+"'")
    cur=conn.cursor()

    print 'fetching from DB...'
    cur.execute("""SELECT session_id, referer, request_url from log_a where request_url not like '%/js/%' AND request_url not like '%/img/%' AND request_url not like '%/css/%' order by folio""")
    nodes=cur.fetchall()
    nodes_length=len(nodes)
    print 'extract sessions...'
    for index in range(0, len(nodes)):
        if(index%100000==0):
            print (index*100)/nodes_length
        session=str(nodes[index][0])
        url_path=urlparse(str(nodes[index][1]))
        if(session in users_path):
            users_path[session]['nav_path'].append(clear_ficha(url_path.path))
            users_path[session]['last_index']=index
        else:
            users_path[session]={'nav_path':[clear_ficha(url_path.path)]}
            users_path[session]['last_index']=index

    print 'sessions extracted successfull'
    print len(users_path)
    session_id_=''
    max_length=0
    print 'creating relations'
    for key in users_path:
        url_path_req=urlparse(str(nodes[users_path[key]['last_index']][2]))
        users_path[key]['nav_path'].append(clear_ficha(url_path_req.path))
        nav_length=len(users_path[key]['nav_path'])
        for idx in range(1,nav_length):
            if(users_path[key]['nav_path'][idx-1]+'->'+users_path[key]['nav_path'][idx] in relations):
                relations[users_path[key]['nav_path'][idx-1]+'->'+users_path[key]['nav_path'][idx]]['count']=relations[users_path[key]['nav_path'][idx-1]+'->'+users_path[key]['nav_path'][idx]]['count']+1
            else:
                relations[users_path[key]['nav_path'][idx-1]+'->'+users_path[key]['nav_path'][idx]]={'count':1,'node0':users_path[key]['nav_path'][idx-1], 'node1':users_path[key]['nav_path'][idx]}
    pretty_relations['links']=[]
    pretty_relations['nodes']=[]
    for edge in relations:
        pretty_relations['links'].append({'source':relations[edge]['node0'],'target':relations[edge]['node1'], 'value':str(relations[edge]['count'])})
        if({'name':relations[edge]['node0']} not in pretty_relations['nodes']):
            pretty_relations['nodes'].append({'name':relations[edge]['node0']})
        if({'name':relations[edge]['node1']} not in pretty_relations['nodes']):
            pretty_relations['nodes'].append({'name':relations[edge]['node1']})
    G=nx.DiGraph()
#    for node in pretty_relations['nodes']:
#        G.add_node(node['name'])
    for node in pretty_relations['links']:
        G.add_edge(node['source'], node['target'], weight=int(node['value']))
#        print 'source = '+node['source']+' & target = '+node['target']
#        print int(node['value'])
    #nx.draw_networkx_edge_labels(G,pos=nx.spring_layout(G))
#    nx.draw_random(G,font_size=9,font_color="#000000", edge_color="#8888FF")
#    plt.show()


    pp=pprint.PrettyPrinter(indent=1)
    pp.pprint(pretty_relations)
             
#    print 'found '+str(len(relations))+' relations'


except psycopg2.Error as e:
    print e.pgerror;
