"""
Author: srinivas.kumarr

Python client for interacting with a server via STOMP over websockets.
"""
# pylint: disable = invalid-name, no-self-use
import thread
import websocket
import stomper
import queue

from framework.lib.nulog import INFO, ERROR

# Since we are Using SockJS fallback on the server side we are directly subscribing to Websockets here.
# Else the url up-till notifications would have been sufficient.
ws_uri = "ws://{}:{}/notifications/websocket"

class StompClient(object):
  """Class containing methods for the Client."""

  NOTIFICATIONS = None

  def __init__(self, jwt_token, pc_ip="127.0.0.1", port_number=8765):
    """
    Initializer for the class.

    Args:
      jwt_token(str): JWT token to authenticate.
      pc_ip(str): Ip of the PC.
      port_number(int): port number through which we want to make the
                        connection.

    """
    self.NOTIFICATIONS = queue.Queue()
    self.headers = {"Authorization": "Bearer " + jwt_token}
    self.ws_uri = ws_uri.format(pc_ip, port_number)

  @staticmethod
  def on_open(ws):
    """
    Handler when a websocket connection is opened.

    Args:
      ws(Object): Websocket Object.

    """
    ws.send("CONNECT\naccept-version:1.0,1.1,2.0\n\n\x00\n")
    sub = stomper.subscribe("/user/queue/alert", "MyuniqueId", ack="auto")
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
    INFO("Received the message: " + str(unpacked_msg))
    self.add_notifications(unpacked_msg)

  def on_error(self, err):
    """
    Handler when an error is raised.

    Args:
      err(str): Error received.

    """
    ERROR("The Error is:- " + err)

  def on_closed(self):
    """
    Handler when a websocket is closed, ends the client thread.
    """
    INFO("The websocket connection is closed.")
    thread.exit()

