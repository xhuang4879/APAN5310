#!/usr/bin/env python3
# -*- coding: utf-8 -*-


'''imports'''
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import numpy as np

def make_identifier(df):
    str_id = df.apply(lambda x: '_'.join(map(str, x)), axis=1)
    return pd.factorize(str_id)[0]
# %% SQL TABLE CREATION

'''connection to pgadmin'''
conn_url = 'postgresql://postgres:123@localhost/nypd_project'
engine = create_engine(conn_url)
connection = engine.connect()

'''create 3NF tables'''
tables = """
	CREATE TABLE sex(
        sex_id VARCHAR(255) PRIMARY KEY,
        sex VARCHAR(255)
        );
 
    CREATE TABLE statistical_murder_flag(
        murder_flag_id VARCHAR(255) PRIMARY KEY,
        murder_flag BOOLEAN
        );
    
    CREATE TABLE age_group(
        age_id VARCHAR(255) PRIMARY KEY,
        age_range VARCHAR(255)
        );
    
    CREATE TABLE race(
        race_id VARCHAR(255) PRIMARY KEY,
        race VARCHAR(255)
        );
    
    CREATE TABLE jurisdiction(
        jurisdiction_code VARCHAR(255) PRIMARY KEY,
        jurisdiction VARCHAR(255)
        );
    
    CREATE TABLE borough(
        borough_id VARCHAR(255) PRIMARY KEY,
        borough VARCHAR(255) 
        );
    
    CREATE TABLE location(
        location_id VARCHAR(255) PRIMARY KEY,
        location_desc VARCHAR(255)
        );
    
    CREATE TABLE perpetrator(
        perp_id VARCHAR(255) PRIMARY KEY,
        sex_id VARCHAR(255) REFERENCES sex(sex_id),
        race_id VARCHAR(255) REFERENCES race(race_id),
        age_id VARCHAR(255) REFERENCES age_group(age_id)
        );
    
    CREATE TABLE coordinate(
        coordinate_id VARCHAR(255) PRIMARY KEY,
        x_coord_cd VARCHAR(255),
        y_coord_cd VARCHAR(255),
        longitude VARCHAR(255),
        latitude VARCHAR(255)
        );
    
    CREATE TABLE victim(
        vic_id VARCHAR(255) PRIMARY KEY,
        sex_id VARCHAR(255) REFERENCES sex(sex_id),
        race_id VARCHAR(255) REFERENCES race(race_id),
        age_id VARCHAR(255) REFERENCES age_group(age_id)
        );
    
    CREATE TABLE precinct(
        precinct VARCHAR(255) PRIMARY KEY,
        phone VARCHAR(255),
        address VARCHAR(255),
        borough_id VARCHAR(255) REFERENCES borough(borough_id)
        );
    
    CREATE TABLE incident(
        incident_key VARCHAR(255),
        precinct VARCHAR(255),
        borough_id VARCHAR(255) REFERENCES borough(borough_id), 
        jurisdiction_code VARCHAR(255) REFERENCES jurisdiction(jurisdiction_code),
        murder_flag_id VARCHAR(255) REFERENCES statistical_murder_flag(murder_flag_id),
        coordinate_id VARCHAR(255) REFERENCES coordinate(coordinate_id),
        location_id VARCHAR(255) REFERENCES location(location_id),
        PRIMARY KEY (incident_key, precinct, borough_id, jurisdiction_code,
                     murder_flag_id, coordinate_id, location_id)
        );
    
    CREATE TABLE incident_perp(
        incident_key VARCHAR(255),
        perp_id VARCHAR(255),
        PRIMARY KEY (incident_key, perp_id),
        FOREIGN KEY (perp_id) REFERENCES perpetrator(perp_id)
        );
    
    CREATE TABLE incident_time(
        incident_key VARCHAR(255) PRIMARY KEY,
        incident_date DATE,
        incident_time TIME
        );
    
    CREATE TABLE incident_vic(
        incident_key VARCHAR(255),
        vic_id VARCHAR(255),
        PRIMARY KEY (incident_key, vic_id),
        FOREIGN KEY (vic_id) REFERENCES victim(vic_id)
        );
    
        """
connection.execute(tables)
connection.close()

# %% ETL PROCESS

'''extraction'''
df = pd.read_csv (r'nypd_shooting.csv')
df = df.rename(columns=str.lower)
pt = pd.read_csv (r'nypd_precinct.csv')
pt = pt.rename(columns=str.lower)


'''transformation'''
# sex
sex = df['perp_sex'].append(df['vic_sex'], ignore_index=True).drop_duplicates().to_frame()
sex.rename(columns={ sex.columns[0]: "sex" }, inplace = True)
sex['sex_id'] = make_identifier(sex[['sex']])

# statistical_murder_flag
statistical_murder_flag = df[['statistical_murder_flag']].drop_duplicates()
statistical_murder_flag = statistical_murder_flag.rename(
    {'statistical_murder_flag': 'murder_flag'}, axis=1)
statistical_murder_flag['murder_flag_id'] = make_identifier(statistical_murder_flag[['murder_flag']])

# age_group
age_group = df['perp_age_group'].append(df['vic_age_group'], ignore_index=True).drop_duplicates().to_frame()
age_group.rename(columns={ age_group.columns[0]: "age_range" }, inplace = True)
age_group['age_id'] = make_identifier(age_group[['age_range']])

# race
race = df['perp_race'].append(df['vic_race'], ignore_index=True).drop_duplicates().to_frame()
race.rename(columns={ race.columns[0]: "race" }, inplace = True)
race['race_id'] = make_identifier(race[['race']])

# jurisdiction
jurisdiction = df[['jurisdiction_code']].drop_duplicates()
jurisdiction['jurisdiction'] = np.where(jurisdiction['jurisdiction_code'] == 0, 'Patrol',
                                        np.where(jurisdiction['jurisdiction_code'] == 1, 'Transit',
                                                 np.where(jurisdiction['jurisdiction_code'] == 2, 'Housing', np.nan)))
jurisdiction = jurisdiction.dropna()

# borough
borough = df[['boro']].drop_duplicates()
borough.rename(columns={borough.columns[0]: "borough" }, inplace = True)
borough['borough_id'] = make_identifier(borough[['borough']])
print(borough)

# location
location = df[['location_desc']].drop_duplicates()
location['location_id'] = make_identifier(location[['location_desc']])

# perpetrator
perpetrator = df[['perp_age_group', 'perp_sex', 'perp_race']].drop_duplicates()
perpetrator = pd.merge(perpetrator,sex, how='left', right_on='sex', left_on='perp_sex')
perpetrator = pd.merge(perpetrator,race, how='left', right_on='race', left_on='perp_race')
perpetrator = pd.merge(perpetrator,age_group, how='left', right_on='age_range', left_on='perp_age_group')
perpetrator = perpetrator.drop(columns=['perp_sex','perp_race','perp_age_group','sex','race','age_range'], axis=1)
perpetrator['perp_id'] = make_identifier(perpetrator[['sex_id','race_id','age_id']])

# coordinate
coordinate = df[['x_coord_cd', 'y_coord_cd', 'longitude', 'latitude']].drop_duplicates()
coordinate['coordinate_id'] = make_identifier(coordinate[['x_coord_cd', 'y_coord_cd', 'longitude', 'latitude']])

# victim
victim = df[['vic_age_group', 'vic_sex', 'vic_race']].drop_duplicates()
victim = pd.merge(victim, sex, how='left', right_on='sex', left_on='vic_sex')
victim = pd.merge(victim, race, how='left', right_on='race', left_on='vic_race')
victim = pd.merge(victim ,age_group, how='left', right_on='age_range', left_on='vic_age_group')
victim = victim.drop(columns=['vic_sex','vic_race','vic_age_group','sex','race','age_range'], axis=1)
victim['vic_id'] = make_identifier(victim[['sex_id','race_id','age_id']])

# precinct
precinct = pt.drop_duplicates()
#precinct['precinct'] = precinct.precinct.str.extract('(\d+)')
precinct = precinct.dropna()
precinct['precinct'] = precinct.precinct.astype(str).str.extract(r'(\d+)')

precinct['borough'] = precinct['borough'].str.upper()
precinct = pd.merge(precinct, borough, how='left', on='borough')
precinct = precinct.drop(columns=['borough'], axis=1)
precinct['precinct'] = precinct['precinct'].astype(str).astype(int)
print('\n precinct')
print(precinct)

# incident
incident = df[['incident_key', 'precinct', 'boro', 'jurisdiction_code',
               'statistical_murder_flag', 'x_coord_cd', 'y_coord_cd',
               'latitude', 'longitude','location_desc']].drop_duplicates()

incident = pd.merge(incident, borough, how='left', left_on='boro', right_on='borough')
incident = pd.merge(incident, statistical_murder_flag, how='left', left_on='statistical_murder_flag', right_on='murder_flag')
incident = pd.merge(incident, coordinate, how='left', on=['x_coord_cd', 'y_coord_cd',
               'latitude', 'longitude'])
incident = pd.merge(incident, location, how='left', on='location_desc')
incident = incident[['incident_key', 'precinct', 'borough_id', 
                     'jurisdiction_code', 'murder_flag_id', 'coordinate_id',
                     'location_id']]
incident = incident.dropna()

# incident_perp
incident_perp = df[['incident_key', 'perp_sex', 'perp_race', 'perp_age_group']]
incident_perp = pd.merge(incident_perp, sex, how='left', left_on='perp_sex', right_on='sex')
incident_perp = pd.merge(incident_perp, race, how='left', left_on='perp_race', right_on='race')
incident_perp = pd.merge(incident_perp, age_group, how='left', left_on='perp_age_group', right_on='age_range')
incident_perp = pd.merge(incident_perp, perpetrator, how='left', on=['sex_id','race_id','age_id'])
incident_perp = incident_perp[['incident_key','perp_id']].drop_duplicates()

# incident_time
incident_time = df[['incident_key','occur_date','occur_time']].drop_duplicates()
incident_time.rename(columns={'occur_date': "incident_date",
                              'occur_time': "incident_time"}, inplace = True)

# incident_vic
incident_vic = df[['incident_key', 'vic_sex', 'vic_race', 'vic_age_group']]
incident_vic = pd.merge(incident_vic, sex, how='left', left_on='vic_sex', right_on='sex')
incident_vic = pd.merge(incident_vic, race, how='left', left_on='vic_race', right_on='race')
incident_vic = pd.merge(incident_vic, age_group, how='left', left_on='vic_age_group', right_on='age_range')
incident_vic = pd.merge(incident_vic, victim, how='left', on=['sex_id','race_id','age_id'])
incident_vic = incident_vic[['incident_key','vic_id']].drop_duplicates()

'''load'''
borough.to_sql('borough', engine, index=False, if_exists="append")
print("loaded borough")
jurisdiction.to_sql('jurisdiction', engine, index=False, if_exists="append")
sex.to_sql('sex', engine, index=False, if_exists="append")
statistical_murder_flag.to_sql('statistical_murder_flag', engine, index=False, if_exists="append")
age_group.to_sql('age_group', engine, index=False, if_exists="append")
race.to_sql('race', engine, index=False, if_exists="append")
print("loaded race")
location.to_sql('location', engine, index=False, if_exists="append")
perpetrator.to_sql('perpetrator', engine, index=False, if_exists="append")
coordinate.to_sql('coordinate', engine, index=False, if_exists="append")
victim.to_sql('victim', engine, index=False, if_exists="append")
print("loaded victim")
precinct.to_sql('precinct', engine, index=False, if_exists="append")
print("loaded precinct")

incident.to_sql('incident', engine, index=False, if_exists="append")
incident_perp.to_sql('incident_perp', engine, index=False, if_exists="append")
incident_vic.to_sql('incident_vic', engine, index=False, if_exists="append")
incident_time.to_sql('incident_time', engine, index=False, if_exists="append")
print("Done with loaded")

connection.close()

