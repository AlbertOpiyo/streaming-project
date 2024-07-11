import random
import time
from datetime import datetime

import psycopg2
import simplejson as json
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, KafkaConfigurationError, KafkaConnectionError


consumer = KafkaConsumer(
    'voters_topic',
    bootstrap_servers='localhost:9092',
    group_id='voting-group',
    auto_offset_reset='earliest',
    enable_auto_commit=False
)
producer = KafkaProducer(
                        bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: json.dumps(v).encode('utf-8')
                        )

from main import delivery_report
# def consume_messages():
#     result = []
#     consumer.subscribe(['candidates_topic'])
#     try:
#         while True:
#             msg = consumer.poll(timeout=1.0)
#             if msg is None:
#                 continue
#             elif msg.error():
#                 if msg.error().code() == KafkaError._PARTITION_EOF:
#                     continue
#                 else:
#                     print(msg.error())
#                     break
#             else:
#                 result.append(json.loads(msg.value().decode('utf-8')))
#                 if len(result) == 3:
#                     return result

#             # time.sleep(5)
#     except KafkaException as e:
#         print(e)


if __name__ == "__main__":
    conn = psycopg2.connect(
            dbname="votingsystem",
            user="postgres",
            password="@Nairobi2003",
            host="localhost",
            port=5432
        )
    cur = conn.cursor()

    # candidates
    candidates_query = cur.execute("""
           SELECT row_to_json(t)
         FROM (
                    SELECT * FROM candidates
        ) t;
    """)
    candidates = cur.fetchall()
    candidates = [candidate[0] for candidate in candidates]
    
    if len(candidates) == 0:
        raise Exception("No candidates found in database")
    else:
        print(candidates)

    consumer.subscribe(['voters_topic'])
    try:
        while True:
            raw_msgs = consumer.poll(timeout_ms=1000)
            if raw_msgs is None:
                continue
            # elif msg.error():
            #     if msg.error().code() == KafkaError._PARTITION_EOF:
            #         continue
            #     else:
            #         print(msg.error())
            #         break
            for tp, messages in raw_msgs.items():
                for msg in messages:
                    try:
                        voter = json.loads(msg.value.decode('utf-8'))
                        chosen_candidate = random.choice(candidates)
                        vote = voter | chosen_candidate | {
                            "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                            "vote": 1
                        }

                        print("User {} is voting for candidate: {}".format(vote['voter_id'], vote['candidate_id']))
                        cur.execute("""
                                INSERT INTO votes (voter_id, candidate_id, voting_time)
                                VALUES (%s, %s, %s)
                            """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))

                        conn.commit()

                        producer.send(
                            'votes_topic',
                            key=vote["voter_id"].encode('utf-8'),
                            value=vote,
                            callback=delivery_report
                        )
                        producer.flush()
                    except Exception as e:
                        print("Error: {}".format(e))
                        # conn.rollback()
                        continue
            time.sleep(0.2)
    except KafkaError as e:
        print(e)
            
    #         else:
    #             voter = json.loads(msg.value().decode('utf-8'))
    #             chosen_candidate = random.choice(candidates)
    #             vote = voter | chosen_candidate | {
    #                 "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    #                 "vote": 1
    #             }

    #             try:
    #                 print("User {} is voting for candidate: {}".format(vote['voter_id'], vote['candidate_id']))
    #                 cur.execute("""
    #                         INSERT INTO votes (voter_id, candidate_id, voting_time)
    #                         VALUES (%s, %s, %s)
    #                     """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))

    #                 conn.commit()

    #                 producer.produce(
    #                     'votes_topic',
    #                     key=vote["voter_id"],
    #                     value=json.dumps(vote),
    #                     on_delivery=delivery_report
    #                 )
    #                 producer.poll(0)
    #             except Exception as e:
    #                 print("Error: {}".format(e))
    #                 # conn.rollback()
    #                 continue
    #         time.sleep(0.2)
    # except KafkaError as e:
    #     print(e)