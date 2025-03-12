from redis import Redis
import time
import logging
import psycopg2
import uuid 
import json
redis_conn = Redis(host="<<hostname>>", db=0, socket_timeout=5, decode_responses=True)
logging.basicConfig(level=logging.INFO)

logging.info('Starting')
while True:
    vote = redis_conn.rpop("votes")    
    if vote is not None:        
        vote_json = json.loads(vote)
        logging.info(vote_json)
        conn = None
        try:
            conn = psycopg2.connect(
                host="<<hostname>>",
                database="<<databasename>>",
                user="<<user>>",
                password="<<password>>")

            logging.info('Connected to postgres.')

            sql = """INSERT INTO votes(id, host, vote) VALUES(%s,%s,%s);"""
            cur = conn.cursor()
            cur.execute(sql, (str(uuid.uuid4()), vote_json['host'], vote_json['vote']))
            # commit the changes to the database
            conn.commit()
            # close communication with the database
            cur.close()            
            
            logging.info('Vote added.')
        except (Exception, psycopg2.DatabaseError) as error: 
            logging.info('Error occured while connecting to postgres.')
            logging.error(error)
        finally:
            if conn is not None:
                conn.close()
                logging.info('Database connection closed.')

    
    logging.info('Sleeping')
    time.sleep(2)