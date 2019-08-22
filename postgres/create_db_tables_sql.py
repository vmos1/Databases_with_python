# -*- coding: utf-8 -*-
"""
Created on Mon May 29 22:02:37 2017

@author: wijay
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
    question by user SingleNegationElimination. The question can and answer
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

    db_name = "lattice_database"
    create_database(db_name=db_name)

    engine = make_engine(echo=True)

    create_ensemble = """
        CREATE TABLE IF NOT EXISTS ensemble
        (
        id serial PRIMARY KEY,
        ns integer,
        nt integer,
        beta numeric,
        kappa numeric DEFAULT 0.0,
        log text,
        notes text,
        UNIQUE(ns, nt, beta, kappa)
        );
        """

    create_raw_data_file = """
        CREATE TABLE IF NOT EXISTS raw_data_file
        (
        id serial PRIMARY KEY,
        filename text,
        abspath text,
        machine text,
        checksum text,
        file_type text,
        birthdate timestamp with time zone,
        parsedate timestamp with time zone,
        last_modified_time timestamp with time zone,
        UNIQUE(filename, abspath, machine)
        );
        """
        
    create_map_hmc = """
        CREATE TABLE IF NOT EXISTS map_hmc
        (
        id serial PRIMARY KEY,
        hasenbusch_on boolean,
        metropolis_on boolean,
        nstep_gauge integer,
        nstep_1 integer,
        nstep_2 integer,
        traj_length numeric,
        cg_tolerance numeric,
        UNIQUE(hasenbusch_on, metropolis_on, nstep_gauge, nstep_1, nstep_2, traj_length, cg_tolerance)
        );
        """

    create_map_qhb = """
        CREATE TABLE IF NOT EXISTS map_qhb
        (
        id serial PRIMARY KEY,
        warms integer,
        qhb_steps integer,
        UNIQUE(warms, qhb_steps)
        );
        """
        
    create_map_generate = """
        CREATE TABLE IF NOT EXISTS map_generate
        (
        id serial PRIMARY KEY,
        hmc_id integer REFERENCES map_hmc(id),
        qhb_id integer REFERENCES map_qhb(id),
        CONSTRAINT branching_junction CHECK (
            (hmc_id IS NOT NULL)::integer + (qhb_id IS NOT NULL)::integer = 1
        )
        );
    """

    create_gauge_config = """
        CREATE TABLE IF NOT EXISTS gauge_config
        (
        id serial PRIMARY KEY,
        ens_id integer REFERENCES ensemble (id) ON DELETE CASCADE,
        rdf_id integer REFERENCES raw_data_file (id) ON DELETE CASCADE,
        prev_id integer REFERENCES gauge_config (id) ON DELETE SET NULL,
        map_id integer REFERENCES map_generate (id),
        traj integer,
        series text,
        checksum text,
        origin text,
        stored boolean,
        exclude boolean,
        notes text,
        UNIQUE(ens_id, traj, series)
        );
        """
        
    create_map_equil_cut = """
        CREATE TABLE IF NOT EXISTS map_equil_cut
        (
        id serial PRIMARY KEY,
        version text,
        label text,
        UNIQUE(version, label)
        );
    """
    
    create_equil_cut = """
        CREATE TABLE IF NOT EXISTS equil_cut
        (
        id serial PRIMARY KEY,
        map_id integer REFERENCES map_equil_cut (id) ON DELETE CASCADE,
        ens_id integer REFERENCES ensemble (id) ON DELETE CASCADE,
        start_traj integer,
        end_traj integer DEFAULT NULL,
        UNIQUE(ens_id, start_traj, end_traj, series)
        );
        """
        
    create_equil_cut_mask = """
        CREATE TABLE IF NOT EXISTS equil_cut_mask
        (
        eq_id integer REFERENCES equil_cut (id) ON DELETE CASCADE,
        gc_id integer REFERENCES gauge_config (id) ON DELETE CASCADE,
        keep boolean DEFAULT FALSE,
        UNIQUE(eq_id, gc_id)
        );
        """
    
    # This part below should probably be a function    
    #SELECT * FROM results_corr_two_point ctp
    #WHERE ctp.gc_id IN (
    #    SELECT gc_id FROM apply_equil_cut
    #    WHERE eq_id=12
    #)
    create_apply_equil_cut = """
        CREATE VIEW IF NOT EXISTS apply_equil_cut AS (
        SELECT eqm.eq_id AS eq_id, gc.id AS gc_id
        FROM gauge_config gc
        LEFT JOIN equil_cut_mask eqm ON gc.id = eqm.gc_id
        WHERE (eqm.keep IS NULL OR eqm.keep = TRUE)
        AND gc.traj >= eq.start_traj
        AND gc.traj <= eq.end_traj
        )
    """
    
    create_map_gauge_diagnostic = """
        CREATE TABLE IF NOT EXISTS map_gauge_diagnostic
        (
        id serial PRIMARY KEY,
        version text,
        UNIQUE(version)    
        );
        """
    
    create_result_gauge_diagnostic = """
        CREATE TABLE IF NOT EXISTS result_gauge_diagnostic
        (
        id serial PRIMARY KEY,
        map_id integer REFERENCES map_gauge_diagnostic (id) ON DELETE CASCADE,
        gc_id integer REFERENCES gauge_config (id) ON DELETE CASCADE,
        rdf_id integer REFERENCES raw_data_file (id) ON DELETE CASCADE,
        plaq_s numeric,
        plaq_t numeric,
        delta_s numeric,
        accepted boolean DEFAULT NULL,
        iter_per_traj numeric DEFAULT NULL,
        UNIQUE(map_id,gc_id)
        );
        """
        
    queries = [
        create_ensemble,
        create_raw_data_file,
        create_map_hmc,
        create_map_qhb,
        create_map_generate,
        create_gauge_config,
        create_map_gauge_diagnostic,
        create_result_gauge_diagnostic,
    ]

    for query in queries:
        engine.execute(query)

    print('Ok, made it to the end.')
        
if __name__ == '__main__':
    main()

