#!/usr/bin/python

#  Labs SQL optimizer
#  v0.9
#  License CC by sa
#
#  Hedonil 2014
#

import threading
import time
import oursql
import re
import pprint
import redis
import json
import cgi
import os
from ConfigParser import SafeConfigParser
from pygments import highlight
from pygments.lexers import MySqlLexer
from pygments.formatters import HtmlFormatter



class ThreadQuery(threading.Thread):

 
    def __init__ (self, type, query ):

        threading.Thread.__init__(self)
        self.type = type
        self.query = query
        self.data = None
        self.error = None

    def run(self):
        global dbmon, dbexec, id
        
        #print self.query
        
        if ( self.type == "mon" ):
            try:
                cursor = dbmon.cursor( oursql.DictCursor )
                time.sleep( 0.2 )
                cursor.execute(self.query, plain_query=True)
                self.data = cursor.fetchall()
                
            except Exception as e:
                self.data = []
                self.error = str(e) + " id: "+ str(id)
            
            finally:
                cursor.execute("kill "+ str(id) )
                
        else:
            try:
                cursor = dbexec.cursor( oursql.Cursor )
                cursor.execute(self.query)
                self.data = 'ok'
                
            except Exception as e:
                self.data = []
                self.error = str(e)+ " id: "+ str(id)
        

        cursor.close()    
        return
        

def dbconn (database):
    host = 's5.labsdb'
    
    parser = SafeConfigParser()
    parser.read('/data/project/tools-info/replica.my.cnf')
    dbUser = parser.get('client', 'user').strip("'")
    dbPwd = parser.get('client', 'password').strip("'")
    
    conn = oursql.connect(host=host, user=dbUser, passwd=dbPwd, db=database, charset='utf8', raise_on_warnings=False, read_timeout=3 )
    
    return conn

def getDbSelect( selectedDb ):
    global myRedis
    
    ttl = 86400
    hash = "toolsinfoSQLOptimizerDBList"
    lc = myRedis.get(hash)
    
    if ( not lc ):
        dbcon = dbconn('meta_p')
        toplist = "('enwiki','dewiki','nlwiki','frwiki','zhwiki','commonswiki','wikidatawiki','centralauth')"
        query = "(Select dbname from wiki where dbname in " + toplist + "order by dbname) UNION (Select dbname from wiki where dbname not in " + toplist +" order by dbname)"
        cursor = dbcon.cursor( oursql.DictCursor )
        cursor.execute( query, plain_query=True)
        dbs = cursor.fetchall()

        if( len(dbs) > 0 ):
            myRedis.setex( hash, json.dumps(dbs), ttl)
        
        cursor.close()
        dbcon.close()
    
    else:
        dbs = json.loads(lc)
    
    
    select = '<optgroup label="Popular">'
    popcount = 0
    for row in dbs:
        if (popcount == 8 ):
            select += '</optgroup><optgroup label="World">'
        
        selected = ''
        if ( row["dbname"] == selectedDb ):
            selected = 'selected'
            
        select += '<option '+ selected +' value="'+ row["dbname"] +'" >'+ row["dbname"] +'</option>'
        popcount += 1
        
    select += '</optgroup>'

    return select

def getRefs( usedDBs, usedTables ):
    
    refs = []
    
    for db in usedDBs:
        dump = open( '/data/project/tools-info/schemadumps/'+ db, 'r' ).read()
        tbls = re.split('Table structure for table', dump)
            
        dump = open( '/data/project/tools-info/schemadumps/views-'+ db, 'r' ).read()
        views = re.split('View:', dump)
        
        for table in usedTables:
            matchTbls = filter(lambda x: re.findall(u'`' + table +'`', x), tbls)
            if ( matchTbls ):
                refs.append({
                       'db': db,
                       'table': table,
                       'tabledef': re.sub('\/\*.*?\*\/;|--|^[ ]*`[_a-z]*?`', '', matchTbls[0]).strip() 
                    })
                
                matchViews = filter(lambda x: re.findall(u'^[\s]*'+ table +'?([ \t]*\n|_logindex|_userindex)', x), views)
                if (matchViews):
                    for matchView in matchViews:
                        viewname = re.split('\n', matchView.strip() )
                        refs.append({
                                'db': 'view',
                                'table': viewname[0],
                                'tabledef': matchView.strip()
                            })
         
    #pprint.pprint(refs)
    
    '''make it some strings'''
    
    refList = '<table>';
    for ref in refs:
        markup = highlight( ref["tabledef"], MySqlLexer(), HtmlFormatter() ) 
        marginLeft = "40px" if(ref["db"] == "view") else "0px"
        refList += '<tr><td><span style="margin-left:'+ marginLeft +'" >' + ref["db"] +'.'+ ref["table"] +'</span></td></tr><tr><td><div style="margin-left:'+ marginLeft +'" >'+ markup +'</div></td></tr>'

    refList += '</table>';
    
    return refList
    
def runExplain( baseDb, query ):
    global dbmon, dbexec, id, usedTables

    dbmon = dbconn( baseDb + '_p' )
    dbexec = dbconn( baseDb + '_p' )
    
    
    curexec = dbexec.cursor( oursql.DictCursor )
    curexec.execute("SELECT CONNECTION_ID() as conid")
    resexec = curexec.fetchone()
    id = int(resexec['conid'])
    curexec.close()
    
    querymon = "Show explain for " + str(id)
    queryexec = query
     
    results = []
    resData = []
     
    current = ThreadQuery( 'exec', queryexec )
    results.append(current)
    current.start()
    current = ThreadQuery( 'mon', querymon )
    results.append(current)
    current.start()  
    
    for res in results:
        res.join()
        resData.append({'type': res.type, 'error': res.error, 'data': res.data})
        
    
    ''' format things '''
    ths = ''
    tds = ''
    output = 'something went wrong'
    keys = ['id','select_type','table','type','possible_keys','key','key_len','ref','rows','Extra']
    #print resData
    for resp in resData:
        
        if (resp["type"] == "mon"):
            
            if ( resp["error"] ):
                return str( resData[0]["error"]) 

            for key in keys:
                ths += '<th style="text-align:left; padding:2px 10px;">'+ key +'</th>'
            
            #print ths
            for row in resp["data"]:
                #print row
                tds += '<tr>'
                for key in keys:
                    if( key == "table" ): 
                        usedTables.append( row[ key ] )
                        #print "bla" + str(row[key])
                    
                    tds += '<td style="max-width:350px; ">'+ str(row[ key ]).replace(',',', ') +'</td>'
                    
                tds += '</tr>';
                
            output = "<table class='table-condensed table-striped table-bordered' style=' font-size:14px;' ><tr> %s </tr> %s </table>" % (ths, tds)
    
    return output


defaultQuery = u'''SELECT page_namespace, page_title, COUNT(*) 
FROM revision
JOIN page ON rev_page = page_id AND page_namespace IN (0,1) AND page_title like 'Liste_der%'
JOIN centralauth_p.globaluser ON gu_name = rev_user_text
WHERE gu_home_db = 'enwiki' AND rev_len > '2000' AND rev_user_text LIKE 'Ada%'
GROUP BY page_namespace, page_title

/* 
Labs SQL Optimizer v0.9 provides real-time analysis of SQL queries.
It helps detecting expensive mistakes as shown in this example (no key hit, using temporary, using filesort).
If you have a longer running query (> ~10 sec), you can also see the execution plan with > SHOW EXPLAIN FOR <thread_id>. Get the thread_id with > SHOW processlist;
*/ '''

html = u'''
<html>
<head>
    <title> SQL Optimizer</title>
    <meta charset="utf-8">
    <link rel="stylesheet" type="text/css" href="static/css/bootstrap.min.css" /> 
    <link rel="stylesheet" type="text/css" href="static/stylenew.css" />
    <link rel="stylesheet" type="text/css" href="pygments.css" />
    <script type="text/javascript" src="static/sortable.js"></script>
    <script type="text/javascript">
        function switchShow( id, elmnt ) {
            var ff = document.getElementById(id);
            if( ff.style.display == "none" || ff.style.display == undefined ) {
                ff.style.display = "block";
                if(elmnt) elmnt.innerHTML = "[hide]";
            }
            else{
                ff.style.display = "none";
                if(elmnt) elmnt.innerHTML = "[show]";
            }
        }
    </script>
    
</head>
<body>
    <div class="container-fluid">
        <br />    
        <div class="panel panel-primary" style="text-align:center">
            <div class="panel-heading">
                <p class="xt-heading-top" >
                    <span style="font-family:comic-sans;font-style:italic">fancy<sup>+</sup> &nbsp;</span> <span> Labs SQL Optimizer </span> <small>v0.9</small>
                    <a style="padding-left:10px;color:white;font-size:16px;" href="//tools.wmflabs.org/tools-info/" > Tools Info</a>
                    <a style="padding-left:2px;color:white;font-size:16px;" href="//tools.wmflabs.org/xtools/" > &middot; XTools!</a>
                    <a style="float:right;margin-left:-50px;padding-top:10px;color:white;font-size:14px;" href="//de.wikipedia.org/wiki/User:Hedonil" >Hedonil</a>
                </p>
            </div>
            <div class="panel-body xt-panel-body-top" >    
                <p>
                    <span>Running schemas: </span>
                    <a href="//tools.wmflabs.org/tools-info/schemas.php?schema=enwiki" >Normal Wiki</a> &middot;
                    <a href="//tools.wmflabs.org/tools-info/schemas.php?schema=commonswiki" >Commons</a> &middot;
                    <a href="//tools.wmflabs.org/tools-info/schemas.php?schema=wikidatawiki" >Wikidata</a> &middot;
                    <a href="//tools.wmflabs.org/tools-info/schemas.php?schema=centralauth" >Centralauth</a>
                    &nbsp;&bull;&nbsp;
                    <span>View definitions: </span>
                    <a href="//tools.wmflabs.org/tools-info/schemas.php?schema=views" >Public views</a>
                    &nbsp;&bull;&nbsp;
                    <span>Run query onlline: </span>
                    <a href="//quarry.wmflabs.org/" >Quarry</a>
                    &nbsp;&bull;&nbsp;
                    <span>Tutorials: </span>
                    <a href="//wikitech.wikimedia.org/wiki/Nova_Resource:Tools/Shared_Resources/MySQL_queries" >SQL-Repo</a> &middot; 
                    <a href="//wikitech.wikimedia.org/wiki/Nova_Resource:Tools/Help#Database_access" >Help SQL</a>
                </p>

                <p class="alert alert-success xt-alert">You are considered <i>SQL specialist 1st class</i>, if your query hits a key and runs efficiently without "Using temporary" / "Using Filesort"  </p>
                <br />
                    <form accept-charset="utf-8" method="post" action="?">
                        <span style="float:left;"> EXPLAIN </span>
                        <span style="border-bottom:1px dotted; " title="Within this base you can leave the tablenames un-prefixed.&#10;eg: SELECT * FROM page&#10;To access other wikis use syntax <schema>.<tblname>&#10;eg: SELECT * FROM page JOIN commonswiki_p.revision ..." >Query-base: </span> &nbsp; 
                        <select name="base" > %s </select>
                        <textarea style="margin-top:5px;" class="form-control" rows=%s  name="text">%s</textarea>
                        <input class="btn btn-large btn-primary" style="margin-top:5px;" type="submit" value="Submit"></input>
                    </form>
    
                                
            <div class="panel panel-default" style="border-color:blue;">
                <div class="panel-heading" style="padding:0px 10px" >
                    <h4  class="topcaption" >Optimizer result <span class="showhide" onclick="javascript:switchShow( \'result\', this )">[hide]</span></h4>
                </div>
                <div class="panel-body" id="result">
                    %s
                </div>
                <div>
                   %s 
                </div>            
            </div>
            
            <div class="panel panel-default">
                <div class="panel-heading" style="padding:0px 10px">
                    <h4  class="topcaption" >Table / Index / View references <span class="showhide" onclick="javascript:switchShow( \'references\', this )">[hide]</span></h4>
                </div>
                <div class="panel-body" id="references">
                    <p class="alert alert-info xt-alert">Please check View definitions, especially if you are using archive, revision or logging tables. If there\'s a conditional for a column - eg: IF((`enwiki...) AS `rev_user_text` - you cannot access the index in the underlying table from this view. In this case, try another one. All &#34;normal wiki\'s&#34; are referred to enwiki.</p>
                    <br />
                    %s
                </div>            
            </div>
        </div>
    </div>
</div>
</body>
</html>
''' 


usedDbs = []
usedTables =[]
outputRows = 12
debugOutput= ''
resExplain = ''
resRefs = ''

myRedis = redis.Redis(host='tools-redis')

form = cgi.FieldStorage()
rawQuery = form.getvalue("text", None )
baseDb = form.getvalue("base", "enwiki")
debug = form.getvalue("debug", None)

usedDbs.append(baseDb)

if( rawQuery and os.environ['REQUEST_METHOD'] == 'POST' ):
    query = re.sub('explain', '', rawQuery, flags=re.IGNORECASE )
    query = re.sub('[\s]from[\s]', ' ,sleep(1) FROM ', rawQuery, count=1, flags=re.IGNORECASE )
    
    if ( re.search('commonswiki_p\.', query, flags=re.IGNORECASE) ):
        usedDbs.append( 'commonswiki')

    if ( re.search('wikidatawiki_p\.', query, flags=re.IGNORECASE) ):
        usedDbs.append( 'wikidatawiki')
    
    if ( re.search('centralauth_p\.', query, flags=re.IGNORECASE) ):
        usedDbs.append( 'centralauth')
        
    #debugOutput = query
    resExplain = runExplain( baseDb, query )
    resRefs = getRefs( usedDbs, usedTables )
    input = rawQuery

elif( rawQuery and os.environ['REQUEST_METHOD'] == 'GET' ):
    input = rawQuery
    
else:
    input = defaultQuery


    
print html % ( getDbSelect(baseDb), outputRows, input, resExplain, debugOutput, resRefs )
    #print HtmlFormatter().get_style_defs('.highlight')



