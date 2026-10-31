"""TCP proxy that adds latency: every chunk is delivered `--rtt-ms / 2` later, in each
direction, so a request/response pair costs one extra round trip.

    python benchmarks/delay_proxy.py --listen 7712 --target 7711 --rtt-ms 60
"""

import argparse
import asyncio


async def pipe(reader, writer, delay):
    loop = asyncio.get_running_loop()
    queue = asyncio.Queue()

    async def deliver():
        while (item := await queue.get()) is not None:
            due, data = item
            await asyncio.sleep(max(0.0, due - loop.time()))
            writer.write(data)
            await writer.drain()
        writer.close()

    sender = asyncio.create_task(deliver())
    try:
        while data := await reader.read(1 << 16):
            queue.put_nowait((loop.time() + delay, data))
    finally:
        queue.put_nowait(None)
        await sender


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--listen", type=int, required=True)
    p.add_argument("--target", type=int, required=True)
    p.add_argument("--rtt-ms", type=float, default=0.0)
    args = p.parse_args()
    delay = args.rtt_ms / 2000

    async def handle(client_reader, client_writer):
        server_reader, server_writer = await asyncio.open_connection("127.0.0.1", args.target)
        await asyncio.gather(
            pipe(client_reader, server_writer, delay),
            pipe(server_reader, client_writer, delay),
            return_exceptions=True,
        )

    server = await asyncio.start_server(handle, "127.0.0.1", args.listen, backlog=4096)
    async with server:
        await server.serve_forever()


asyncio.run(main())
