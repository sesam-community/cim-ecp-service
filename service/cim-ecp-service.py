#!/usr/bin/env python3
"""
    # cim-service
      - Sesam micro-service for publishing CIM messages over AMQP to EDX Toolbox destined for other ECP endpoint.



    ## environment variables
      - Settings for contacting remote ECP over AMQP.

    ### `CIM_ECP_PORT`
      - AMQP port for remote EDX service. Default CIM_ECP_PORT="5672".

    ### `CIM_ECP_HOST`
      - IP-address or hostname for remote EDX service.

    ### `CIM_ECP_URL`
      - Optional complete AMQP URL which overrides CIM_ECP_PORT and CIM_ECP_HOST variables. E.g CIM_ECP_URL="amqp://127.0.0.1:5672"

    ### `CIM_ECP_PUBLISH`
      - AMQP queue where to publish message on remote service. E.g CIM_ECP_QUEUE_SEND="edx.endpoint.outbox"

    ### `CIM_ECP_REPLY`
      - AMQP queue where to receive replies. E.g CIM_ECP_QUEUE_REPLY="edx.endpoint.reply"

    ### `CIM_ECP_RECIPIENT`
      - Name (string) - recipient/destination endpoint - AKA "receiverCode", see section 5.3.2 in "ENTSO-E EDX User Guide 1.4.0".

    ### `CIM_ECP_LABEL`
      - Publishing application label/tag/type - AKA "businessType", see section 5.3.2 in "ENTSO-E EDX User Guide 1.4.0".

    ### `CIM_ECP_SENDER`
      - Name (string) for sending application - AKA "senderApplication", see section 5.3.2 in "ENTSO-E EDX User Guide 1.4.0".

    ### `CIM_ECP_PREFIX`
      - Prefix (string) when publishing messages - AKA "baMessageID", see section 5.3.2 in "ENTSO-E EDX User Guide 1.4.0".

    ### `CIM_ECP_CONTEXT`
      - Messaging context prefix (string) when publishing messages - AKA "baCorrelationID", see section 5.3.2 in "ENTSO-E EDX User Guide 1.4.0".

    ### `CIM_ECP_SESSION`
      - Context (string) for AMQP session  - AKA "correlation-id", see section 5.3.2 in "ENTSO-E EDX User Guide 1.4.0".

    ### `LOG_LEVEL`
      - Controls logging output verbosity, including for other imported modules (e.g `urllib3` used by `requests`).



    ## API
      - supports HTTP methods GET and POST

    ### /
      - POST sends a given JSON array of CIM entities with a XML transform (document in the `xml` attribute of each entity) and returns a JSON array with message details for each sent entity.


    ### /read
      - GET returns a JSON array of received messages on the ECP platform

    ### /api/schemas/<schema>/<id>
      - GET returns
      - POST updates or inserts



    ## module overview
      - 

    ## tests



    ## running locally
      - `FLASK_APP=cim-amqp-service.py FLASK_ENV=development python -m flask run`  -- for `cherrypy` local micro-http-server and hot-reload on file changes/saves
      - `.env` file in same directory as this source file to set local development environment variables

"""



# basic module support for local development environment
import os
# dotfile = os.path.dirname(__file__) +'/.env'
# if os.path.isfile(dotfile):
#   from dotenv import load_dotenv
#   load_dotenv(dotfile)

# generic Python utility modules
import logging
import json
from datetime import datetime, timedelta      # for Sesam `since` ability in micro-service
import hashlib                                # for Sesam `since` ability in micro-service, detect "real/actual changes"
import secrets                                # for Sesam `since` ability in micro-service, initialise hash-values with random "salt" value
import copy                                   # used for deep copy

# core Sesam micro-service Python frameworks
from flask import Flask, request, Response    # for micro-service API routing
import requests                               # for Sesam node API access and instrospection from micro-service

# AMQP
from proton import Message                    # for AMQP messaging
from proton.utils import BlockingConnection
from proton.handlers import IncomingMessageHandler
import xmltodict
# import ssl
import uuid
import threading
import time


# initialise basic micro-service
app = Flask(__name__)
log_level = logging.getLevelName(os.environ.get("LOG_LEVEL", "ERROR").upper())
# log_level = logging.getLevelName("ERROR")
logging.basicConfig(level=log_level)
log = logging.getLogger(__name__)
log.info('Initialisation...')
print("LOG_LEVEL = %s" % log_level)

# secure hashing
SALT = secrets.token_urlsafe(128) # random string of length ~~ 1.3 * 128

# settings dependent on environment variables
API = os.environ.get("SESAM-API", "")
if API.endswith('/'):
  API = API[:-1]
TOKEN = os.environ.get("SESAM-JWT", "")
SESSION = requests.session()
SESSION.headers.update({'Authorization': 'Bearer ' + TOKEN})


CIM_ECP_HOST = os.environ.get('CIM_ECP_HOST', None) or os.environ.get('CIM_EDX_HOST', "127.0.0.1")
CIM_ECP_PORT = os.environ.get('CIM_ECP_PORT', None) or os.environ.get('CIM_EDX_PORT', "5672")
CIM_ECP_URL = os.environ.get('CIM_ECP_URL', None) or os.environ.get('CIM_EDX_URL', None)
CIM_ECP_PUBLISH = os.environ.get('CIM_ECP_PUBLISH', None) or os.environ.get('CIM_EDX_PUBLISH', None)
CIM_ECP_REPLY = os.environ.get('CIM_ECP_REPLY', None) or os.environ.get('CIM_EDX_REPLY', None)
CIM_ECP_LABEL = os.environ.get('CIM_ECP_LABEL', None) or os.environ.get('CIM_EDX_LABEL', None)
CIM_ECP_SENDER = os.environ.get('CIM_ECP_SENDER', None) or os.environ.get('CIM_EDX_SENDER', None)
CIM_ECP_RECIPIENT = os.environ.get('CIM_ECP_RECIPIENT', None) or os.environ.get('CIM_EDX_RECIPIENT', None)
CIM_ECP_PREFIX = os.environ.get('CIM_ECP_PREFIX', None) or os.environ.get('CIM_EDX_PREFIX', None)
CIM_ECP_CONTEXT = os.environ.get('CIM_ECP_CONTEXT', None) or os.environ.get('CIM_EDX_CONTEXT', None)
CIM_ECP_SESSION = os.environ.get('CIM_ECP_SESSION', None) or os.environ.get('CIM_EDX_SESSION', None)


# CIM_ECP_PUBLISH = "edx.endpoint.inbox.fos"
CIM_ECP_REPLY = "edx.endpoint.inbox.fos"


URL = CIM_ECP_URL or "amqp://%s:%d" % ( CIM_ECP_HOST, int(CIM_ECP_PORT) )




def ecp_replies(queue=CIM_ECP_REPLY, url=URL, label=CIM_ECP_LABEL, sender=CIM_ECP_SENDER, msgid=None, msgctx=None, session=None):
  log.debug('ENTER: ecp_replies(queue=' + json.dumps(queue) + ',url=' + json.dumps(url) + ',label=' + json.dumps(label) + ',sender=' + json.dumps(sender) + ',msgid=' + json.dumps(msgid) + ',msgctx=' + json.dumps(msgctx) + ',session=' + json.dumps(session) + ')')
  result = []
  CONNECTION = None
  CHANNEL = None
  try:
    CONNECTION = BlockingConnection(URL, timeout=60)
    CHANNEL = CONNECTION.create_receiver(address=queue, credit=100)

    count = -1
    try:
      count = len(CHANNEL.fetcher.incoming)
    except Exception as e:
      count = 0
    print('Queue %s has %i message(s).' % (queue, count))
    # log.error('Queue %s has %i message(s).' % (queue, count))

    received = 0
    while count > 0:
      msg = CHANNEL.receive(timeout=25)
      if msg is not None:
        body = msg.body
        properties = msg.properties
        part = {}
        # print("TEST # %d" % frame.delivery_tag)
        for k,v in properties.items():
          part[str(k)] = str(v)
        part['correlation_id'] = str(msg.correlation_id)
        # part['correlation_id'] = str(properties["correlation_id"])
        part['body'] = body.decode('utf-8')
        part['received'] = "%sZ" % datetime.now().isoformat()
        part['_id'] = str(uuid.uuid4())
        details = {}
        try:
          details = xmltodict.parse(body.decode('utf-8'))
        except Exception as e:
          log.error('EXCEPTION -- malformed XML reply')
          log.error(e)
        result.append({**part, **details})
        received += 1
        # Acknowledge the message -- careful, check the props first -- e.g other recipients
        CHANNEL.accept()
        # Escape out of the loop after <count> messages
      count -= 1
      # count = len(CHANNEL.fetcher.incoming)

    if count > 0:
      print('Requeued %i messages' % count)
      # log.error('Requeued %i messages' % count)
    CHANNEL.close()
    CONNECTION.close()
  except Exception as e:
    print('EXCEPTION -- ecp_replies')
    print(e)
    log.error(e)
    # result['error'] = str(e)
    if CHANNEL is not None:
      CHANNEL.close()
    if CONNECTION is not None:
      CONNECTION.close()
  # print('RETURNING: ' + str(result))
  return result


# def ecp_publish(channel, msg, queue=CIM_ECP_PUBLISH, recipient=CIM_ECP_RECIPIENT, label=CIM_ECP_LABEL, sender=CIM_ECP_SENDER, msgid=None, msgctx=None, session=None):
def ecp_publish(msg, queue=CIM_ECP_PUBLISH, recipient=CIM_ECP_RECIPIENT, label=CIM_ECP_LABEL, sender=CIM_ECP_SENDER, msgid=None, msgctx=None, session=None):
  log.debug('ENTER: ecp_publish(msg=#' + str(len(msg)) + ',queue=' + json.dumps(queue) + ',recipient=' + json.dumps(recipient) + ',label=' + json.dumps(label) + ',sender=' + json.dumps(sender) + ',msgid=' + json.dumps(msgid) + ',msgctx=' + json.dumps(msgctx) + ',session=' + json.dumps(session) + ')')
  result = {}
  CONNECTION = None
  CHANNEL = None
  try:
    CONNECTION = BlockingConnection(URL, timeout=25)
    CHANNEL = CONNECTION.create_sender(address=queue)

    correlation_id = str(uuid.uuid4())
    result = {
      'correlation_id': correlation_id,
      'queue': queue,
      'send': "%sZ" % datetime.now().isoformat()
    }
    props = {
      'baMessageId': str(uuid.uuid4()),
      'baCorrelationId': correlation_id, # prefer to use the same AMQP message correlation id as the AMQP application-property "baCorrelationId"
      'businessType': CIM_ECP_LABEL,
      'receiverCode': CIM_ECP_RECIPIENT,
      'senderApplication': CIM_ECP_SENDER
    }
    for k,v in props.items():
      result[k] = v
    # channel.basic_publish(
    CHANNEL.send(
      Message(
        body = msg,
        correlation_id = correlation_id,
        properties = props,
        creation_time = int(time.time()),
        durable = True)
      , timeout = False)
    result['published'] = "%sZ" % datetime.now().isoformat()
    # print('PUBLISHED: ' + str(result))
    CHANNEL.close()
    CONNECTION.close()
  except Exception as e:
    print('EXCEPTION -- ecp_publish')
    print(e)
    log.error(e)
    result['error'] = str(e)
    if CHANNEL is not None:
      CHANNEL.close()
    if CONNECTION is not None:
      CONNECTION.close()
  # print('RETURNING: ' + str(result))
  return result




#####################################################################################################################################################################


# prevent setting globals twice (ie. polling Sesam node twice on startup) when running Flask locally (solution used by Werkzeug themselves)
if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
  print('Starting...')
  log.info('Starting...')

print("CIM_ECP_HOST = %s" % CIM_ECP_HOST)
print("CIM_ECP_PORT = %s" % CIM_ECP_PORT)
print("CIM_ECP_PUBLISH = %s" % CIM_ECP_PUBLISH)
print("CIM_ECP_REPLY = %s" % CIM_ECP_REPLY)
print("CIM_ECP_LABEL = %s" % CIM_ECP_LABEL)
print("CIM_ECP_SENDER = %s" % CIM_ECP_SENDER)
print("CIM_ECP_RECIPIENT = %s" % CIM_ECP_RECIPIENT)
print("CIM_ECP_PREFIX = %s" % CIM_ECP_PREFIX)
print("CIM_ECP_CONTEXT = %s" % CIM_ECP_CONTEXT)
print("CIM_ECP_SESSION = %s" % CIM_ECP_SESSION)

print("CIM_ECP_URL = %s" % CIM_ECP_URL)

print("URL = %s" % URL)


CONNECTION = None
CHANNEL = None
count = -1
address = "CIM_ECP_PUBLISH"
try:
  CONNECTION = BlockingConnection(URL, timeout=25)
  CHANNEL = CONNECTION.create_receiver(address=CIM_ECP_PUBLISH, credit=100)
  count = len(CHANNEL.fetcher.incoming)
  address = str(CHANNEL.link.remote_source.address)
except Exception as e:
  print(e)
  count = 0
if CHANNEL is not None:
  CHANNEL.close()
if CONNECTION is not None:
  CONNECTION.close()
str_amqp = "AMQP: %s # %d " % (CIM_ECP_PUBLISH, count)
print(str_amqp + address)

CONNECTION = None
CHANNEL = None
count = -1
address = "CIM_ECP_REPLY"
try:
  CONNECTION = BlockingConnection(URL, timeout=25)
  CHANNEL = CONNECTION.create_receiver(address=CIM_ECP_REPLY, credit=100)
  count = len(CHANNEL.fetcher.incoming)
  address = str(CHANNEL.link.remote_source.address)
except Exception as e:
  print(e)
  count = 0
if CHANNEL is not None:
  CHANNEL.close()
if CONNECTION is not None:
  CONNECTION.close()
str_amqp = "AMQP: %s # %d " % (CIM_ECP_REPLY, count)
print(str_amqp + address)





#####################################################################################################################################################################


# default route
@app.route('/', methods=['GET'])
def query_default():
  log.debug('ENTER: query_default()')
  result = ecp_replies()
  return Response(result, mimetype='application/json')


# default route
@app.route('/', methods=['POST'])
def publish_default():
  log.debug('ENTER: publish_default()')
  body = request.json
  if body is None:
    return Response('[]', mimetype='application/json')
  result = []
  for item in body:
    xml = str(item['xml'])
    print(xml)
    part = ecp_publish(xml)
    part['_id'] = part['baCorrelationId']
    part['_updated'] = "%sZ" % datetime.now().isoformat()
    # result.append(part.copy())
    result.append(part)
  return Response(json.dumps(result), mimetype='application/json')


# default route
@app.route('/read', methods=['POST','GET'])
def read_replies():
  log.debug('ENTER: read_replies()')
  # body = request.json
  result = ecp_replies(CIM_ECP_REPLY)
  # result = ecp_replies(CIM_ECP_PUBLISH)
  print(result)
  return Response(json.dumps(result), mimetype='application/json')


# test locally
@app.route('/test', methods=['POST'])
def test_publish():
  log.debug('ENTER: test_publish()')
  body = request.json
  if body is None:
    return Response('[]', mimetype='application/json')
  result = []
  for item in body:
    xml = str(item['xml'])
    # item['ecp-publish'] = ecp_publish(CHANNEL, xml)
    item['ecp-publish'] = ecp_publish(xml)
    result.append(item)
  return Response(json.dumps(result), mimetype='application/json')




if __name__ == '__main__':
  CHERRY = os.environ.get("CHERRY", None)
  log.info("CHERRY=%s" % CHERRY)
  CHERRY = True if CHERRY and ( CHERRY.upper() == "TRUE" or int(CHERRY) == 1 )  else False
  if not CHERRY:
    app.run(debug=True, host='0.0.0.0', port=5000)
    # app.run(debug=False, host='0.0.0.0', port=5000)
  else:
    log.info("Using Cherrypy")
    import cherrypy                               # local development stand-alone HTTP server
    cherrypy.tree.graft(app, '/')

    # Set the configuration of the web server to production mode
    cherrypy.config.update({
      'environment': 'production',
      'engine.autoreload_on': False,
      'log.screen': True,
      'server.socket_port': 5000,
      'server.socket_host': '0.0.0.0'
    })


    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()

