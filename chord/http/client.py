import argparse
import logging

from chord.node import RemoteChordNode
from chord.http.transport import HttpChordTransportFactory


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Runs a Chord server.")
    parser.add_argument("hostname", type=str, help="The server hostname.")
    parser.add_argument("port", type=int, help="The server port")

    command_group = parser.add_mutually_exclusive_group()
    command_group.add_argument("--node", action='store_true',
            help="Returns the node info.")
    command_group.add_argument("--create", action='store_true',
            help="Creates a new node ring.")
    command_group.add_argument("--find_successor", type=str,
            help="Finds the successor for a given bucket.")
    command_group.add_argument("--join", type=str,
            help="Joins this node to a node ring")
    command_group.add_argument("--notify", type=str,
            help="Notifies a node that a node might be its predecessor")
    command_group.add_argument("--predecessor", action='store_true',
            help="Returns the predecessor node")
    command_group.add_argument("--successor_list", action='store_true',
            help="Returns the node's successor list")
    command_group.add_argument("--shutdown", action='store_true',
            help="Shuts the node down gracefully")
    command_group.add_argument("--get", type=str,
            help="Returns the stored value for a key")
    command_group.add_argument("--put", type=str,
            help="Stores a value in Chord (format 'key=value')")

    args = parser.parse_args()

    hostname = args.hostname
    port = args.port

    node_id = f"{hostname}:{port}"


    transport_factory = HttpChordTransportFactory()
    node = RemoteChordNode(transport_factory, node_id)

    if args.node:
        logger.info("Getting node info from [%s]", node)
        logger.info(node.node())
    elif args.create:
        logger.info("Creating node ring at [%s]", node)
        logger.info(node.create())
    elif args.find_successor:
        key = args.find_successor
        logger.info("Finding successor for [%s] starting at [%s]", key, node)
        logger.info(node.find_successor(args.find_successor))
    elif args.join:
        remote_node = RemoteChordNode(transport_factory, args.join)
        logger.info("Joining [%s] to node [%s]", node, remote_node)
        logger.info(node.join(remote_node))
    elif args.notify:
        remote_node = RemoteChordNode(transport_factory, args.join)
        logger.info("Notifying [%s] of node [%s]", node, remote_node)
        logger.info(node.notify(remote_node))
    elif args.predecessor:
        logger.info("Getting predecessor info from [%s]", node)
        logger.info(node.get_predecessor())
    elif args.successor_list:
        logger.info("Getting successor list from [%s]", node)
        logger.info(node.get_successor_list())
    elif args.shutdown:
        logger.info("Shutting node [%s] down gracefully", node)
        node.shutdown()
    elif args.get:
        logger.info("Getting key [%s]", args.get)
        logger.info(node.get(args.get))
    elif args.put:
        key, value = args.put.split("=", 1)
        logger.info("Putting key [%s] = value [%s]", key, value)
        logger.info(node.put(key, value))
    else:
        logger.error("Must specify a command")
