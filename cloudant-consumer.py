#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(pika.URLParameters('amqp://xxxxxxxxxx'))
channel = connection.channel()
import json, traceback, cloudant,json,hashlib
for  method, properties, body in channel.consume('dst_queue_event_cloudant'):
	payload = json.loads(body)
  	try:
                key = hashlib.sha1(json.dumps(payload))
                key_hash = key.hexdigest()
                USERNAME = "xxxxxx"
                PASSWORD = "xxxxxx"
                account = cloudant.Account(USERNAME)
                login = account.login(USERNAME, PASSWORD)
                coudantdb = account.database('novodb_event')
                doc = coudantdb.document(key_hash)
                body = {
                        'Nome': '%s' % payload["nome"],
                        'Idade': '%s' % payload["idade"],
                        'Nacionalidade': '%s' % payload["nacionalidade"],
                        }
                resp = doc.put(params=body)
		if method.delivery_tag == 5:
                        break
                channel.basic_ack(method.delivery_tag)
        except:
                print traceback.format_exc()

requeued_messages = channel.cancel()
print('Requeued %i messages' % requeued_messages)
channel.close()
connection.close()
