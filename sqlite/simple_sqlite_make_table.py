# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 16:43:27 2017

@author: vpa
"""

import sqlite3

conn=sqlite3.connect("example.db")
c=conn.cursor()


create_ensemble = """
    CREATE TABLE IF NOT EXISTS ensemble
    (
    id serial PRIMARY KEY,
    LX integer,
    LY integer,
    U numeric,
    log text,
    notes text,
    UNIQUE(LX, LY, U)
    );
    """
create_rhomono="""

    CREATE TABLE IF NOT EXISTS rho_mono
    (
    id serial PRIMARY KEY,
    ensid serial REFERENCES ensemble(id),
    rho numeric,
    notes text,
    UNIQUE (id)
    );
    """
create_sus1="""
    CREATE TABLE IF NOT EXISTS sus1
    (
    id serial PRIMARY KEY,
    ensid serial REFERENCES ensemble(id),
    sus1 numeric,
    notes text,
    UNIQUE (id)
    );

    """        
    
create_sus2="""
    CREATE TABLE IF NOT EXISTS sus2
    (
    id serial PRIMARY KEY,
    ensid serial REFERENCES ensemble(id),s
    sus2 numeric,
    notes text,
    UNIQUE (id)
    );

    """         
    
queries = [
    create_ensemble,
    create_rhomono,
    create_sus1,
    create_sus2
]


c.execute(create_ensemble)

# Add data to dbase

insert_line='''
          INSERT INTO ensemble VALUES(1,4,4,0.4, 'pythoncentral','')
          '''
c.execute(insert_line)




# Read line from Dbase



conn.commit()
conn.close()




