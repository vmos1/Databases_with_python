# -*- coding: utf-8 -*-
"""
Created on Thu Dec 14 14:37:21 2017

@author: vpa

Borrowed code from William Jay
"""


# SQLAlchemy 
import sqlalchemy as sqla
# Password and access information 
import db_settings

def make_engine(NEED_PASSWORD=True,**kwargs):
    """
    Makes an Engine object for interacting with SQL database.
    Useful for SQLAlchemy and for pandas sql functions.
    """
    if NEED_PASSWORD:    
        engine = sqla.create_engine(
            'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'.format(**db_settings.DATABASE),
            **kwargs
        )
    else:
        engine = sqla.create_engine(
            'postgresql+psycopg2://{user}@{host}:{port}/{database}'.format(**db_settings.DATABASE),
            **kwargs
        )
    return engine

def create_database(db_name="lattice_database"):
    """
    Creates a database when none exists before.
    Args:
        name: str, the name for the new database. Default is "test_database"
    Returns:
        None
    
    Remarks:
    This function is essentially copied from an answer to a StackOverflow 
    question by user SingleNegationElimination. The question and answer
    are:
    
    stackoverflow.com/questions/6506578/how-to-create-a-new-database-using-sqlalchemy    
    """
    engine = sqla.create_engine("postgres://postgres@/postgres")
    conn = engine.connect()
    conn.execute("commit")
    
    conn.execute("create database {db_name}".format(db_name=db_name))
    conn.close()

def main():
    print("Creating an example database for storing the results of lattice simulations.")
    print('Running SQLAlchemy version {0}'.format(sqla.__version__))

    db_name ="d1_dbase"
    create_database(db_name=db_name)

    engine = make_engine(echo=True)

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

    for query in queries:
        engine.execute(query)

    print('Ok, made it to the end.')
        
if __name__ == '__main__':
    main()

