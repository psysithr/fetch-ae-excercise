import psycopg2
from psycopg2.sql import SQL, Identifier
from psycopg2.extensions import AsIs
import json
import re

# Establish a connection to the database
conn = psycopg2.connect(
    host="localhost",
    database="jakobnicolasora",
    user="jakobnicolasora",
    password=""
)

def create_table(table_name, fields):
    cur.execute(SQL("drop table if exists {}").format(Identifier(table_name)))
    conn.commit()
    
    fields_sql = [] 
    for f in fields:
        fields_sql.append( SQL( "{} varchar" ).format(Identifier(f)))
    
    create_query = SQL("create table if not exists {tbl_name} ( {fields_sql_execute} )").format(
        tbl_name = Identifier(table_name),
        fields_sql_execute = SQL(', ').join( fields_sql ))

    cur.execute(create_query)
    conn.commit()

def load_table(table_name):
    with open(table_name + ".json", "r") as f:
        data = f.read().splitlines()
    f.close()

    query_sql = SQL("insert into {} select * from json_populate_recordset(NULL::{}, %s)").format(
        Identifier(table_name), Identifier(table_name))

    cur.execute(query_sql, (str(data).replace("'{", "{").
                                        replace("}'","}").
                                        replace("\\'","'").
                                        replace('\\"','').
                                        replace('\\',''),))

    conn.commit()


cur = conn.cursor()

schemas = [('receipts',['_id','bonusPointsEarned','bonusPointsEarnedReason','createDate',
                        'dateScanned','finishedDate','modifyDate','pointsAwardedDate',
                        'pointsEarned','purchaseDate','purchasedItemCount',
                        'rewardsReceiptItemList','rewardsReceiptStatus',
                        'totalSpent','userId']),

            ('users', ['_id','state','createdDate', 'lastLogin','role','active']),

            ('brands',['_id','barcode','brandCode','category','categoryCode','cpg',
                        'topBrand','name'])]

for s in schemas:
    create_table(s[0],s[1])
    load_table(s[0])
