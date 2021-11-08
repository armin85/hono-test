from __future__ import print_function, unicode_literals
import threading
import time
from proton import Message
from proton.reactor import Container
from amqp import AmqpSender, AmqpReceiver


biz_app_uri = f'amqp://localhost:15672'
device_uri = f'amqp://localhost:5672'
tenantId = 'ea8b6601-6fb7-4fb5-a097-2d9a3cdea0d8'
deviceId = 'b932fb15-fdbd-4c12-9ed7-40aaa8763412'

biz_app_user = 'consumer@HONO'
biz_app_pw = 'verysecret'
device_user = f'{deviceId}@{tenantId}'
device_pw = 'my-secret-password'

correlation_id = 'myCorrelationId'
command_reply_to = f'command_response/{tenantId}/{correlation_id}'


print("Business application subscribing for command replies-------------------------------------------")
cr_container = Container(AmqpReceiver(biz_app_uri, command_reply_to, biz_app_user, biz_app_pw))
cr_thread = threading.Thread(target=lambda: cr_container.run(), daemon=True)
cr_thread.start()

print("Device subscribing for commands-------------------------------------------------------------------")
c_container = Container(AmqpReceiver(device_uri, f'command', device_user, device_pw))
c_thread = threading.Thread(target=lambda: c_container.run(), daemon=True)
c_thread.start()
#some delay to start up the connection
time.sleep(2)

print("Business application sending a command------------------------------------------------------------")
msg = Message(
    address=f'command/{tenantId}/{deviceId}',
    reply_to=command_reply_to,
    correlation_id=correlation_id,
    content_type="text/plain",
    subject="call",
    body="Hello Bob!"
)
#as in example https://stackoverflow.com/questions/64698271/difficulty-in-sending-amqp-1-0-message
Container(AmqpSender(biz_app_uri, [msg], biz_app_user, biz_app_pw, address=f'command/{tenantId}')).run()

time.sleep(6)
print("Device stops listeing for commands----------------------------------------------------------------")
c_container.stop()
c_thread.join(timeout=5)
print("Business application stops listening for telemetry & events---------------------------------------")
cr_container.stop()
cr_thread.join(timeout=5)
print("everything stopped")
