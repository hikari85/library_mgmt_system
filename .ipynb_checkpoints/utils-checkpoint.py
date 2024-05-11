def get_con_and_cur(dbname="LibMS"):
    import psycopg2 as pg
    con=pg.connect(host="localhost",port="5432",database=dbname,user="postgres",password="pg@postgres")
    cur=con.cursor()
    return con,cur

def list_all_tables(cur):
    cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
    tables = [t[0] for t in cur.fetchall()]
    return tables

def delete_all_tables(con,cur):
    tables=list_all_tables(cur)
    for table in tables:
        try:
            cur.execute(f"""DROP TABLE IF EXISTS {table} CASCADE""")
            con.commit()
        except Exception as e:
            print(e)
            con.rollback()

def clear_table(con,cur,tbname):
    try:
        cur.execute(f"DELETE FROM {tbname}")
        print(f"All Records in Table '{tbname}' Deleted")
        con.commit()
    except Exception as e:
        print(e)
        con.rollback()

def delete_table(con,cur,tbname):
    try:
        cur.execute(f"DROP TABLE IF EXISTS {tbname}")
        print(f"Table '{tbname}' Deleted Successfully")
        con.commit()
    except Exception as e:
        print(e)
        con.rollback()