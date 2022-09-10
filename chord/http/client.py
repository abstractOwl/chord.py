import logging

import click

from chord.constants import (
        NODE, CREATE, FIND_SUCCESSOR, JOIN, NOTIFY, GET_PREDECESSOR, GET_SUCCESSOR_LIST, SHUTDOWN,
        GET_KEY, PUT_KEY
)
from chord.model import (
        NodeRequest, CreateRequest, FindSuccessorRequest, JoinRequest, NotifyRequest,
        GetPredecessorRequest, GetSuccessorListRequest, ShutdownRequest, GetKeyRequest,
        PutKeyRequest
)
from chord.node import RemoteChordNode
from chord.http.transport import HttpChordTransport


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

transport = HttpChordTransport(5)


@click.group()
@click.pass_context
@click.argument("hostname", required=True)
@click.argument("port", required=True)
def cli(ctx, hostname, port):
    ctx.ensure_object(dict)
    node_id = f"{hostname}:{port}"
    ctx.obj["NODE"] = RemoteChordNode(transport, node_id)


@cli.command(CREATE)
@click.pass_context
def create(ctx):
    node = ctx.obj["NODE"]
    logger.info("Creating node ring at [%s]", node)
    logger.info(node.create(CreateRequest()))

@cli.command(NODE)
@click.pass_context
def node_info(ctx):
    node = ctx.obj["NODE"]
    logger.info("Getting node info from [%s]", node)
    logger.info(node.node(NodeRequest()))

@cli.command(FIND_SUCCESSOR)
@click.pass_context
@click.argument("key", required=True)
def find_successor(ctx, key):
    node = ctx.obj["NODE"]
    logger.info("Finding successor for [%s] starting at [%s]", key, node)
    logger.info(node.find_successor(FindSuccessorRequest(key)))


@cli.command(JOIN)
@click.pass_context
@click.argument("hostname", required=True)
@click.argument("port", required=True)
def join(ctx, hostname, port):
    node = ctx.obj["NODE"]
    node_id = f"{hostname}:{port}"
    remote_node = RemoteChordNode(transport, node_id)
    logger.info("Joining [%s] to node [%s]", node, remote_node)
    logger.info(node.join(JoinRequest(remote_node)))

@cli.command(NOTIFY)
@click.pass_context
@click.argument("hostname", required=True)
@click.argument("port", required=True)
def notify(ctx, hostname, port):
    node = ctx.obj["NODE"]
    node_id = f"{hostname}:{port}"
    remote_node = RemoteChordNode(transport, node_id)
    logger.info("Notifying [%s] of node [%s]", node, remote_node)
    logger.info(node.notify(NotifyRequest(remote_node)))

@cli.command(GET_PREDECESSOR)
@click.pass_context
def get_predecessor(ctx):
    node = ctx.obj["NODE"]
    logger.info("Getting predecessor info from [%s]", node)
    logger.info(node.get_predecessor(GetPredecessorRequest()))

@cli.command(GET_SUCCESSOR_LIST)
@click.pass_context
def get_successor_list(ctx):
    node = ctx.obj["NODE"]
    logger.info("Getting successor list from [%s]", node)
    logger.info(node.get_successor_list(GetSuccessorListRequest()))

@cli.command(SHUTDOWN)
@click.pass_context
def shutdown(ctx):
    node = ctx.obj["NODE"]
    logger.info("Shutting node [%s] down gracefully", node)
    logger.info(node.shutdown(ShutdownRequest()))

@cli.command(GET_KEY)
@click.pass_context
@click.argument("key", required=True)
def get_key(ctx, key):
    node = ctx.obj["NODE"]
    logger.info("Getting key [%s]", key)
    logger.info(node.get(GetKeyRequest(key)))

@cli.command(PUT_KEY)
@click.pass_context
@click.argument("key", required=True)
@click.argument("value", required=True)
def put_key(ctx, key, value):
    node = ctx.obj["NODE"]
    logger.info("Putting key [%s] = value [%s]", key, value)
    logger.info(node.put(PutKeyRequest(key, value)))


if __name__ == '__main__':
    cli(obj={})
