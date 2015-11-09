#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(pika.URLParameters('amqp://xxxxxxxxx'))
channel = connection.channel()
import json, traceback, redis,json,hashlib
for  method, properties, body in channel.consume('dst_queue_event_redis'):
	payload = json.loads(body)
  	try:
       		key = hashlib.sha1(json.dumps(payload))
       		key_hash = key.hexdigest()
       		rd=redis.Redis('www.rcsousa.com.br','6379')
       		out = rd.lpush("cadastros_event" , '%s;%s' %(payload["nome"],key_hash))
       		print "[*] message "+body+" received and processed"
		print method.delivery_tag
		if method.delivery_tag == 5:
        		break
		channel.basic_ack(method.delivery_tag)
  	except:
		print traceback.format_exc()

requeued_messages = channel.cancel()
print('Requeued %i messages' % requeued_messages)
channel.close()
connection.close()

