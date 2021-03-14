import argparse
import logging

from chord.node import RemoteChordNode


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Runs a Chord server.")
    parser.add_argument("hostname", type=str, help="The server hostname.")
    parser.add_argument("port", type=int, help="The server port")
    parser.add_argument("ringSize", type=int, help="The Chord ring size")

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

    args = parser.parse_args()

    hostname = args.hostname
    port = args.port
    ring_size = args.ringSize

    node_id = f"{hostname}:{port}"

    node = RemoteChordNode(node_id, ring_size)

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
        remote_node = RemoteChordNode(args.join, ring_size)
        logger.info("Joining [%s] to node [%s]", node, remote_node)
        logger.info(node.join(remote_node))
    elif args.notify:
        remote_node = RemoteChordNode(args.join, ring_size)
        logger.info("Notifying [%s] of node [%s]", node, remote_node)
        logger.info(node.notify(remote_node))
    elif args.predecessor:
        logger.info("Getting predecessor info from [%s]", node)
        logger.info(node.predecessor)
    else:
        logger.error("Must specify a command")
