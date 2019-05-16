import time
import argparse
from confluent_kafka import Producer



parser = argparse.ArgumentParser('''

''',formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("-f","--file", help="file", required=True)
parser.add_argument("-b","--brokers", help="borkers default localhost:9092", default='localhost:9092')
parser.add_argument("-t","--topic", help="topic", required=True)
parser.add_argument("-s","--sleep", help="sleep def 0.1", default=0.1)

args = parser.parse_args()
conf = {
    'bootstrap.servers': args.brokers
    }
sleep_time = float(args.sleep)
topic = args.topic
p = Producer(conf)
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or2 flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))


cur = 0
while True:
    p.poll(0)
    try:
        with open(args.file,'r') as f:
            f.seek(0,2)
            if f.tell() < cur:
                f.seek(0,0)
            else:
                f.seek(cur,0)
            for line in f:
                p.produce(topic, line.strip() , callback=delivery_report)
                time.sleep(sleep_time)
                print(line)
            cur = f.tell()
            
            p.flush()
    except FileNotFoundError as e:
        print(e)
    
    time.sleep(1)
    
