import concurrent
import threading
import time
import sys
import zmq
import socket
from queue import Queue
from threading import Thread

from sawtooth_sdk.consensus.driver import Driver
from sawtooth_sdk.consensus.engine import StartupState
from sawtooth_sdk.consensus.engine import PeerMessage
from sawtooth_sdk.consensus.zmq_service import ZmqService
from sawtooth_sdk.consensus import exceptions
from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.protobuf import consensus_pb2
from sawtooth_sdk.protobuf.validator_pb2 import Message


class GiskardTester:
    def __init__(self, num_endpoints):
        print("init tester")
        """ Create Streams for each engine in the net for exchanging nstates with each other """
        self._exit = False
        self.num_endpoints = num_endpoints
        #self.streams = []
        #self._stream = Stream(self.tester_endpoint(0))
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect('tcp://127.0.0.1:3030')
        self.socket.setsockopt_string(zmq.SUBSCRIBE, '')

        print("\n\n\nTester socket bound\n\n\n")
        #for i in range(0, num_endpoints):
        #    self.streams.append(Stream(self.tester_endpoint(i)))
        t1 = threading.Thread(target=self.tester_loop, args=())
        t1.start()

    @staticmethod
    def tester_endpoint(num):
        return 'tcp://127.0.0.2:{}'.format(3030)

    def tester_loop(self):
        while True:
            print("\n\nWaiting for message\n\n")
            message = self.socket.recv_pyobj()
            print("\n\n\nTester Msg received ", message, "\n\n\n")

        """try:
            #future = self._stream.receive()
            while True:
                #if self._exit:
                #    break
                try:
                    future = self._stream._send_recieve_thread._sock.listen()
                    print("\n\n\nTried stream receive\n\n\n\n")
                    message = future.result(1)
                    print("\n\n\nTried future result\n\n\n\n")
                except concurrent.futures.TimeoutError:
                    continue
                try:

                    result = self._process(message)
                    print("\n\n\nTESTER got a msg\n\n\n\n")
                    # if message was a ping ignore
                    if result[0] == Message.PING_REQUEST:
                        continue

                    #self._updates.put(result)
                    print(str(result))

                except exceptions.ReceiveError as err:
                    sys.stderr.write("%s", err)
                    continue
        except Exception:  # pylint: disable=broad-except
            sys.stderr.write("Uncaught driver exception")"""

    def _process(self, message):
        type_tag = message.message_type

        if type_tag == Message.CONSENSUS_NOTIFY_PEER_CONNECTED:
            notification = consensus_pb2.ConsensusNotifyPeerConnected()
            notification.ParseFromString(message.content)

            data = notification.peer_info

        elif type_tag == Message.CONSENSUS_NOTIFY_PEER_DISCONNECTED:
            notification = consensus_pb2.ConsensusNotifyPeerDisconnected()
            notification.ParseFromString(message.content)

            data = notification.peer_id

        elif type_tag == Message.CONSENSUS_NOTIFY_PEER_MESSAGE:
            notification = consensus_pb2.ConsensusNotifyPeerMessage()
            notification.ParseFromString(message.content)

            header = consensus_pb2.ConsensusPeerMessageHeader()
            header.ParseFromString(notification.message.header)

            peer_message = PeerMessage(
                header=header,
                header_bytes=notification.message.header,
                header_signature=notification.message.header_signature,
                content=notification.message.content)

            data = peer_message, notification.sender_id

        elif type_tag == Message.CONSENSUS_NOTIFY_BLOCK_NEW:
            notification = consensus_pb2.ConsensusNotifyBlockNew()
            notification.ParseFromString(message.content)

            data = notification.block

        elif type_tag == Message.CONSENSUS_NOTIFY_BLOCK_VALID:
            notification = consensus_pb2.ConsensusNotifyBlockValid()
            notification.ParseFromString(message.content)

            data = notification.block_id

        elif type_tag == Message.CONSENSUS_NOTIFY_BLOCK_INVALID:
            notification = consensus_pb2.ConsensusNotifyBlockInvalid()
            notification.ParseFromString(message.content)

            data = notification.block_id

        elif type_tag == Message.CONSENSUS_NOTIFY_BLOCK_COMMIT:
            notification = consensus_pb2.ConsensusNotifyBlockCommit()
            notification.ParseFromString(message.content)

            data = notification.block_id

        elif type_tag == Message.CONSENSUS_NOTIFY_ENGINE_DEACTIVATED:
            self.stop()
            data = None

        elif type_tag == Message.PING_REQUEST:
            data = None

        else:
            raise exceptions.ReceiveError(
                'Received unexpected message type: {}'.format(type_tag))

        self._stream.send_back(
            message_type=Message.CONSENSUS_NOTIFY_ACK,
            correlation_id=message.correlation_id,
            content=consensus_pb2.ConsensusNotifyAck().SerializeToString())

        return type_tag, data



    """future = self._stream.send(
            message_type=Message.CLIENT_BLOCK_GET_BY_TRANSACTION_ID_REQUEST,
            content=ClientBlockGetByTransactionIdRequest(
                transaction_id=transaction_id).SerializeToString())

        content = future.result().content

        response = ClientBlockGetResponse()
        response.ParseFromString(content)

        if response.status == ClientBlockGetResponse.NO_RESOURCE:
            raise ValueError("The transaction supplied is not in a block")

        block = response.block

        header = BlockHeader()
        header.ParseFromString(block.header)

        consensus_block = ConsensusBlock(
            block_id=bytes.fromhex(block.header_signature),
            previous_id=bytes.fromhex(header.previous_block_id),
            signer_id=bytes.fromhex(header.signer_public_key),
            block_num=header.block_num,
            payload=header.consensus,
            summary=b'')

        poet_block = PoetBlock(consensus_block)

        return poet_block"""