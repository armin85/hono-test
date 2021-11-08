from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container


class Amqp(MessagingHandler):
    def __init__(self, server, address, user, password, options=None):
        super(Amqp, self).__init__()
        self.server = server
        self.address = address
        self.user = user
        self.password = password
        self.options = options

    def create_connection(self, event):
        return event.container.connect(
            self.server,
            sasl_enabled=True,
            allowed_mechs="PLAIN",
            allow_insecure_mechs=True,
            user=self.user,
            password=self.password
        )

    def on_connection_error(self, event):
        print("Connection Error")

    def on_link_error(self, event):
        print("Link Error")

    def on_transport_error(self, event):
        print("Transport Error")


class AmqpReceiver(Amqp):
    def __init__(self, server, address, user, password, options=None):
        super(AmqpReceiver, self).__init__(server, address, user, password, options)
        self.server = server
        self.user = user
        self.password = password

    def on_start(self, event):
        print("Connecting Receiver")
        conn = self.create_connection(event)
        print("Receiver Connection established")
        event.container.create_receiver(context=conn, source=self.address, options=self.options)
        print("Receiver created")

    def on_message(self, event):
        print(f'Receiver [{self.address}] got message:')
        print(f'  {event.message.properties}')
        print(f'  {event.message.body}')
        #just for test purposes - the device sends imediatelly the reply if a reply_to is given
        if event.message.reply_to is not None:
            reply_to = event.message.reply_to.split('/')
            tenant_id = reply_to[1]
            device_id = reply_to[2]
            resp = Message(
                address=event.message.reply_to,
                correlation_id=event.message.correlation_id,
                content_type="text/plain",
                properties={
                    'status': 200,
                    'tenant_id': tenant_id,
                    'device_id': device_id
                },
                body=f'Reply to: {event.message.body}'
            )
            Container(AmqpSender(self.server, [resp], self.user, self.password)).run()


class AmqpSender(Amqp):
    def __init__(self, server, messages, user, password, address=None, options=None):
        super(AmqpSender, self).__init__(server, address, user, password, options)
        self.messages = messages

    def on_start(self, event):
        print("Connecting Sender")
        conn = self.create_connection(event)
        print("Sender Connection established")
        event.container.create_sender(context=conn, target=self.address)
        print("Sender created")

    def on_sendable(self, event):
        print("In Msg send")
        for msg in self.messages:
            event.sender.send(msg)
        event.sender.close()
        event.connection.close()
        print("Sender & connection closed")
