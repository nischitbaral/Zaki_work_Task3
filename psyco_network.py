

import json
import psycopg2

with open('in_net.json', 'r') as f:
    data = json.load(f)

conn = psycopg2.connect(
    dbname="my_pgdb",
    user="postgres",
    password="admin",
    host="localhost",
    port=5432
)
conn.autocommit = True

cur = conn.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS network_data (
    bC TEXT,
    bCT TEXT,
    negA TEXT,        
    bCls TEXT,
    negR DOUBLE PRECISION,
    poSH INTEGER[],     
    provider_group_id BIGINT[],  
    negT TEXT,
    mdH TEXT[]
);
""" 
cur.execute(create_table_query)

insert_query = """
INSERT INTO network_data (
    bc, bcT,negA,bCls,poSH,negR,provider_group_id,negT,mdH
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for item in data:
    bC = item.get('bC', 'default_value')
    bcT=item.get('bCT', 'default_value')
    negA = item.get('negA', 'default_value')
    bCls = item.get('bCls', 'default_value')
    poSH = item.get('poSH', None) 
    negR = item.get('negR', 0.0)  
    provider_group_id = item.get('provided_group_id',[])
    negT = item.get('negT', 'default_value')
    mdH = item.get('mdH', None)
    cur.execute(insert_query, (bC,bcT,negA,bCls,poSH,negR,provider_group_id,negT,mdH))

conn.commit()
cur.close()
conn.close()

