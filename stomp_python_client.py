"""
Author: srinivas.kumarr

Python client for interacting with a server via STOMP over websockets.
"""
import thread
import websocket
import stomper
import queue

# Since we are Using SockJS fallback on the server side we are directly subscribing to Websockets here.
# Else the url up-till notifications would have been sufficient.
ws_uri = "ws://{}:{}/notifications/websocket"

class StompClient(object):
  """Class containing methods for the Client."""

  # Notifications queue, which will store all the mesaages we receive from the server.
  NOTIFICATIONS = None

  #Do note that in this case we use jwt_token for authentication hence we are 
  #passing the same in the headers, else we can pass encoded passwords etc. 
  def __init__(self, jwt_token, server_ip="127.0.0.1", port_number=8765, destinations=[]):
    """
    Initializer for the class.

    Args:
      jwt_token(str): JWT token to authenticate.
      server_ip(str): Ip of the server.
      port_number(int): port number through which we want to make the
                        connection.
      destination(list): List of topics which we want to subscribe to.

    """
    self.NOTIFICATIONS = queue.Queue()
    self.headers = {"Authorization": "Bearer " + jwt_token}
    self.ws_uri = ws_uri.format(server_ip, port_number)

  @staticmethod
  def on_open(ws):
    """
    Handler when a websocket connection is opened.

    Args:
      ws(Object): Websocket Object.

    """
    #Iniitial CONNECT required to initialize the server's client registries.
    ws.send("CONNECT\naccept-version:1.0,1.1,2.0\n\n\x00\n")
    
    # Subscribing to all required desitnations. 
    for destination in destinations:
      sub = stomper.subscribe(destination, "clientuniqueId", ack="auto")
      ws.send(sub)

  def create_connection(self):
    """
    Method which starts of the long term websocket connection.
    """

    ws = websocket.WebSocketApp(self.ws_uri, header=self.headers,
                                on_message=self.on_msg,
                                on_error=self.on_error,
                                on_close=self.on_closed)
    ws.on_open = self.on_open
    
    # Run until interruption to client or server terminates connection. 
    ws.run_forever()

  def add_notifications(self, msg):
    """
    Method to add a message to the websocket queue.

    Args:
      msg(dict): Unpacked message received from stomp watches.

    """
    self.NOTIFICATIONS.put(msg)

  def on_msg(self, msg):
    """
    Handler for receiving a message.

    Args:
      msg(str): Message received from stomp watches.

    """
    frame = stomper.Frame()
    unpacked_msg = stomper.Frame.unpack(frame, msg)
    print("Received the message: " + str(unpacked_msg))
    self.add_notifications(unpacked_msg)

  def on_error(self, err):
    """
    Handler when an error is raised.

    Args:
      err(str): Error received.

    """
    print("The Error is:- " + err)

  def on_closed(self):
    """
    Handler when a websocket is closed, ends the client thread.
    """
    print("The websocket connection is closed.")
    thread.exit()

