import asyncio
import base64
import json
import logging
from io import BytesIO
from os import getcwd, chdir
from time import sleep
import datetime
import tempfile

import aiohttp
from aiohttp.client_exceptions import ClientConnectorError

logger = logging.getLogger(__name__)

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 9222
PDF_OPTIONS = {
    'landscape',           # boolean - Paper orientation. Defaults to false.
    'displayHeaderFooter', # boolean -Display header and footer. Defaults to false.
    'printBackground',     # boolean - Print background graphics. Defaults to false.
    'scale',               # number - Scale of the webpage rendering. Defaults to 1.
    'paperWidth',          # number - Paper width in inches. Defaults to 8.5 inches.
    'paperHeight',         # number - Paper height in inches. Defaults to 11 inches.
    'marginTop',           # number - Top margin in inches. Defaults to 1cm (~0.4 inches).
    'marginBottom',        # number - Bottom margin in inches. Defaults to 1cm (~0.4 inches).
    'marginLeft',          # number - Left margin in inches. Defaults to 1cm (~0.4 inches).
    'marginRight',         # number - Right margin in inches. Defaults to 1cm (~0.4 inches).
    'pageRanges'           # string - Paper ranges to print, e.g., '1-5, 8, 11-13'. Defaults to the empty string, which means print all pages.
}


async def get_debug_url(session, host=DEFAULT_HOST, port=DEFAULT_PORT):
    debug_url = 'http://%s:%s/json/list' % (host, port)
    logger.debug('Getting debug URL...')
    for i in range(10):
        try:
            async with session.get(debug_url) as resp:
                assert resp.status == 200
                resp = json.loads(await resp.text())
                return resp[0]['webSocketDebuggerUrl']
        except ClientConnectorError:
            await asyncio.sleep(0.5)
            if i == 9:
                raise


async def receive_messages(ws, pending_requests):
    async for msg in ws:
        if msg.type == aiohttp.WSMsgType.TEXT:
            result = json.loads(msg.data)
            if 'error' in result:
                raise Exception(str(result))
            method = result.get('method')
            if method == 'Network.requestWillBeSent':
                pending_requests.add(result['params']['requestId'])
            elif method == 'Network.loadingFinished':
                pending_requests.remove(result['params']['requestId'])
            # print(result)
            yield result
        elif msg.type == aiohttp.WSMsgType.CLOSED:
            break
        elif msg.type == aiohttp.WSMsgType.ERROR:
            break


async def wait_until(worker, method=None, id=None):
    if method is not None:
        key = 'method'
        val = method
    else:
        key = 'id'
        val = id
    async for msg in worker.iter:
        if msg.get(key) == val:
            return msg


async def wait_for_network(worker, timeout=20):
    delta = datetime.timedelta(seconds=timeout)
    time = datetime.datetime.now()

    while True:
        sleep(0.2)

        # send dummy request and wait until response
        id1 = worker.send('Network.enable')
        await wait_until(worker, id=id1)

        if not worker.pending_requests:
            break
        else:
            print('Pending requests, waiting')

        if time + delta < datetime.datetime.now():
            break


class MessageWorker:

    def __init__(self, ws):
        self.ws = ws
        self.pending_requests = set()
        self.count = 0
        self.iter = receive_messages(self.ws, self.pending_requests)

    def send(self, method, **params):
        self.count += 1
        msg = {'id': self.count, 'method': method, 'params': params}
        self.ws.send_str(json.dumps(msg))
        return self.count


async def send_print_command(ws, print_url, **options):
    params = {x: y for x, y in options.items() if x in PDF_OPTIONS}

    worker = MessageWorker(ws)

    worker.send('Page.enable')
    worker.send('Network.enable')
    worker.send('Page.navigate', url=print_url)
    await wait_until(worker, method='Page.frameStoppedLoading')
    await wait_for_network(worker, 20)
    msgid = worker.send('Page.printToPDF', **params)
    result = await wait_until(worker, id=msgid)
    await ws.close()
    return base64.b64decode(result['result']['data'])


async def wait_for_port(ip, port, num_tries=3, timeout=5, loop=None):
    fut = asyncio.open_connection(ip, port, loop=loop)
    writer = None
    try:
        for tries in range(num_tries):
            try:
                _, writer = await asyncio.wait_for(fut, timeout=timeout)
            except asyncio.TimeoutError:
                logger.debug('Timeout %d connecting to chrome...', tries + 1)
            except Exception as exc:
                logger.debug('Error {}:{} {}'.format(ip, port, exc))
            logger.debug("{}:{} Connected".format(ip, port))
            break
    finally:
        if writer is not None:
            writer.close()


async def get_pdf(url, loop=None, host=DEFAULT_HOST, port=DEFAULT_PORT, **options):
    if loop is None:
        loop = asyncio.get_event_loop()
    async with aiohttp.ClientSession(loop=loop) as session:
        debugger_url = await get_debug_url(session, host=host, port=port)
        logger.debug('Connecting to %s', debugger_url)
        async with session.ws_connect(debugger_url) as ws:
            pdf_bytes = await send_print_command(ws, url, **options)
            if pdf_bytes is None:
                raise Exception('Could not get PDF.')
            return BytesIO(pdf_bytes)


class ChromeContextManager:
    def __init__(self, loop=None, chrome_binary='/usr/local/bin/chromium',
                             host=DEFAULT_HOST, port=9222, **options):
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop
        self.chrome_binary = chrome_binary
        self.host = host
        self.port = port

    async def __aenter__(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        tmpname = self.temp_dir.name
        self.args = [
            self.chrome_binary,
            '--headless',
            '--disable-gpu',
            '--no-sandbox',
            '--single-process',
            '--data-path=' + tmpname,
            '--homedir=' + tmpname,
            '--disk-cache-dir=' + tmpname,
            '--remote-debugging-port=%s' % self.port,
            '--remote-debugging-address=%s' % self.host,
            '--user-data-dir=%s' % tmpname
        ]
        logger.debug('Starting chrome: %s', self.chrome_binary)
        cwd = getcwd()
        chdir(tmpname)
        try:
            self.proc = await asyncio.create_subprocess_exec(*self.args,
                                                             loop=self.loop)
        finally:
            chdir(cwd)
        logger.debug('Started, waiting for debug port...')
        await wait_for_port(self.host, self.port, loop=self.loop)
        logger.debug('Debug port available.')

    async def __aexit__(self, exc_type, exc, tb):
        logger.debug('Terminating chrome...')
        self.proc.terminate()
        await self.proc.wait()
        self.temp_dir.cleanup()
        logger.debug('Chrome terminated.')


async def get_pdf_with_chrome(url, loop=None, params=None, **options):
    if loop is None:
        loop = asyncio.get_event_loop()
    async with ChromeContextManager(loop, **options):
        return await get_pdf(url, loop, **options)


def get_pdf_sync(url, params=None, **options):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(get_pdf(url, loop=loop, **options))


def get_pdf_with_chrome_sync(url, params=None, **options):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(get_pdf_with_chrome(url, loop=loop,
                                                       **options))
