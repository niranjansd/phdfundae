import numpy as np
import sqlite3
from lxml import etree
import pandas as pd

def nsfparse(xmlfile):
    ''' Parse nsf xml file '''
##iterparse directly makes a list/dict out of everything, every element
##destroy parent child configurations. parse preserves them all.
##xpath (//fieldname) is fastest way if only specific fields are to be found.
##int is 32 bit, more than 2 billion. I dont think i need more than that.
##maybe keywords can have bigint., even that is not reqd.

    doc = etree.parse(xmlfile)
    awardict = {}
    PIdict = []
    Instdict = {}

    awardict['AwardID'] = int(doc.find('//AwardID').text)
    awardict['AwardAmount'] = int(doc.find('//AwardAmount').text)
    awardict['AwardTitle'] = doc.find('//AwardTitle').text
    awardict['AwardInstrument'] = doc.find('//AwardInstrument/Value').text
    awardict['AwardEffectiveDate'] = doc.find('//AwardEffectiveDate').text
    awardict['AwardExpirationDate'] = doc.find('//AwardExpirationDate').text
    awardict['AbstractNarration'] = doc.find('//AbstractNarration').text

    PIs = doc.findall('//Investigator')
    for PI in PIs:
        tempdict = {}
        tempdict['FirstName'] = PI.find('FirstName').text
        tempdict['LastName'] = PI.find('LastName').text
        tempdict['EmailAddress'] = PI.find('EmailAddress').text
        tempdict['RoleCode'] = PI.find('RoleCode').text
        PIdict.append(tempdict)
    awardict['Investigators'] = PIdict

    Inst = doc.find('//Institution')
    for child in Inst:
        Instdict[child.tag] = child.text    
    awardict['Institution'] = Instdict
    
    return awardict

def sqlinit(conn):
    c = conn.cursor()

    c.execute("""drop table if exists Grants""")
    c.execute("""drop table if exists PIs""")
    c.execute("""drop table if exists Data""")

    conn.commit()

    c.execute("""create table Grants (
            gid     INTEGER PRIMARY KEY NOT NULL,
            ID    int ,
            AAmount int ,
            ADate text ,
            EDate text ,
            Instrument text)""")

    c.execute("""create table PIs (
            pid     INTEGER PRIMARY KEY NOT NULL,
            gid    int ,
            FirstName     text ,
            LastName     text ,
            Email     text ,
            Role   text)""")

    c.execute("""create table Data (
            did    INTEGER PRIMARY KEY NOT NULL,
            gid    int,
            Abstract    text,
            Title   text)""")

    c.close()

def sqlsave(conn,adict):

    c = conn.cursor()

##to use autoincrement of primary key, you have to address the columns
##of the table and submit values to each column but omit the prmary key

    c.execute("""INSERT INTO Grants(ID,AAmount,ADate,EDate,Instrument)
               VALUES (?,?,?,?,?)""",
              (adict['AwardID'],adict['AwardAmount'],
               adict['AwardEffectiveDate'],adict['AwardExpirationDate'],
               adict['AwardInstrument']))
##link the other databases to the first one using the row id.
##not using awardid as key since those might differ between grant agencies.
    lastid = c.lastrowid
    for PI in adict['Investigators']:
        c.execute("""INSERT INTO PIs(gid,FirstName,LastName,Email,Role)
               VALUES (?,?,?,?,?)""",
                  (lastid,PI['FirstName'],PI['LastName'],
                   PI['EmailAddress'],PI['RoleCode']))

    c.execute("""insert into Data(gid,Abstract,Title) VALUES (?,?,?)""",
              (lastid,adict['AbstractNarration'],adict['AwardTitle']))

    conn.commit()

#    c.execute ("""select AAmount,LastName,Title from Grants left join PIs on PIs.gid = Grants.gid""")

    for row in c:
        print (row)

    c.close()


if __name__ == "__main__":
    conn = sqlite3.connect("C:/Users/niranjan/Dropbox/Research/Programming/Apps/Data/phdfundata/NSFGrants/database1")

##    awardict = nsfparse("C:/Users/niranjan/Dropbox/Research/Programming/Apps/Data/phdfundata/NSFGrants/All/7200045.xml")
##    print(awardict)
###    sqlinit(conn)
##    sqlsave(conn,awardict)
    
    sqlinit(conn)
    pid=0
    glist = []
    dlist = []
    plist = []
    for i in range(401894):
        adict = nsfparse("C:/Users/niranjan/Dropbox/Research/Programming/Apps/Data/phdfundata/NSFGrants/All/1 ("+str(i+1)+").xml")

## changing the script to go through pandas dataframe, sqlsave not used
## sql inserting 400000 files one by one was tkaing too long, so trying to
## make 1000 file long dataframes and inserting sql 1000 at a time.
        gdict = {'gid':i+1,'ID':adict['AwardID'],'AAmount':adict['AwardAmount'],
               'ADate':adict['AwardEffectiveDate'],'EDate':adict['AwardExpirationDate'],
               'Instrument':adict['AwardInstrument']}
        glist.append(gdict)
        ddict = {'did':i+1,'gid':i+1,'Abstract':adict['AbstractNarration'],
                    'Title':adict['AwardTitle']}
        dlist.append(ddict)
        for PI in adict['Investigators']:
            pid=pid+1
            pdict = {'pid':pid,'gid':i+1,'FirstName':PI['FirstName'],
                     'LastName':PI['LastName'],
                       'Email':PI['EmailAddress'],'Role':PI['RoleCode']}
            plist.append(pdict)
        
        if (i >= 401893) or not i%2000:
            print(i)

            gdf1=pd.DataFrame(glist)
            ddf1=pd.DataFrame(dlist)
            pdf1=pd.DataFrame(plist)
            gdf = gdf1.set_index('gid')
            ddf = ddf1.set_index('did')
            pdf = pdf1.set_index('pid')

            gdf.to_sql(con=conn,name='Grants',if_exists='append',flavor='sqlite')
            ddf.to_sql(con=conn,name='Data',if_exists='append',flavor='sqlite')
            pdf.to_sql(con=conn,name='PIs',if_exists='append',flavor='sqlite')

            glist = []
            dlist = []
            plist = []

#find list of tables
    tableListQuery = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY Name"
    c.execute(tableListQuery)
    tables = map(lambda t: t[0], c.fetchall())        
#count number of rows in table
    c.execute("""select count() from Grants""")
    numrows=c.fetchone()
#count number of columns
    columnsQuery = "PRAGMA table_info(%s)" % table
    c.execute(columnsQuery)
    numberOfColumns = len(c.fetchall())





