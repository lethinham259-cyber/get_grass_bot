import asyncio
import random
import ssl
import json
import time
import uuid
from loguru import logger
from websockets_proxy import Proxy, proxy_connect
from fake_useragent import UserAgent

ip_retry_count = {}
user_agent = UserAgent()
max_retries = 3

async def connect_to_wss(proxy_url, user_id, random_user_agent):
    device_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, proxy_url))
    logger.info(f"Device ID: {device_id} using proxy: {proxy_url}")
    ip_retry_count[device_id] = 0
    
    while True:
        try:
            await asyncio.sleep(random.randint(1, 3))
            custom_headers = {
                "User-Agent": random_user_agent,
                "Origin": "chrome-extension://ilehaonighjijnmpnagapkhpcdbhclfg",
            }
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            uri = "wss://proxy.wynd.network:4444/"
            server_hostname = "proxy.wynd.network"
            
            proxy = Proxy.from_url(proxy_url)
            async with proxy_connect(uri, proxy=proxy, ssl=ssl_context, server_hostname=server_hostname,
                                     extra_headers=custom_headers) as websocket:
                async def send_ping():
                    while True:
                        send_message = json.dumps(
                            {"id": str(uuid.uuid4()), "version": "1.0.0", "action": "PING", "data": {}})
                        await websocket.send(send_message)
                        await asyncio.sleep(15)

                asyncio.create_task(send_ping())
                while True:
                    response = await websocket.recv()
                    message = json.loads(response)
                    logger.info(message)
                    if message.get("action") == "AUTH":
                        auth_response = {
                            "id": message["id"],
                            "origin_action": "AUTH",
                            "result": {
                                "browser_id": device_id,
                                "user_id": user_id,
                                "user_agent": custom_headers['User-Agent'],
                                "timestamp": int(time.time()),
                                "device_type": "extension",
                                "version": "4.0.1"
                            }
                        }
                        await websocket.send(json.dumps(auth_response))

                    elif message.get("action") == "PONG":
                        pong_response = {"id": message["id"], "origin_action": "PONG"}
                        await websocket.send(json.dumps(pong_response))
        except Exception as e:
            ip_retry_count[device_id] += 1
            logger.error(f"Error with proxy {proxy_url}: {str(e)} (Retry {ip_retry_count[device_id]}/{max_retries})")
            if ip_retry_count[device_id] > max_retries:
                logger.error(f"Max retries exceeded for proxy {proxy_url}. Removing it.")
                remove_error_proxy(proxy_url)
                if device_id in ip_retry_count:
                    del ip_retry_count[device_id]
                return None
            await asyncio.sleep(5)

async def main():
    _user_id = "3JDf1yPR7ceCnFGtoBWyaHPJJdT"
    proxy_file = 'proxy.txt'
    with open(proxy_file, 'r') as file:
        all_proxies = [line.strip() for line in file.read().splitlines() if line.strip()]

    if not all_proxies:
        logger.error("No proxies found in proxy.txt!")
        return

    num_proxies_to_use = min(len(all_proxies), 10)
    active_proxies = random.sample(all_proxies, num_proxies_to_use)
    
    tasks = {asyncio.create_task(connect_to_wss(proxy, _user_id, user_agent.random)): proxy for proxy in active_proxies}

    while True:
        done, pending = await asyncio.wait(tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if task.result() is None:
                failed_proxy = tasks[task]
                logger.info(f"Removing and replacing failed proxy: {failed_proxy}")
                if failed_proxy in active_proxies:
                    active_proxies.remove(failed_proxy)
                if all_proxies:
                    new_proxy = random.choice(all_proxies)
                    active_proxies.append(new_proxy)
                    new_task = asyncio.create_task(connect_to_wss(new_proxy, _user_id, user_agent.random))
                    tasks[new_task] = new_proxy
            tasks.pop(task)
        for proxy in set(active_proxies) - set(tasks.values()):
            random_user_agent = user_agent.random
            new_task = asyncio.create_task(connect_to_wss(proxy, _user_id, random_user_agent))
            tasks[new_task] = proxy

def remove_error_proxy(proxy):
    try:
        with open("proxy.txt", "r+") as file:
            lines = file.readlines()
            file.seek(0)
            for line in lines:
                if line.strip() != proxy:
                    file.write(line)
            file.truncate()
    except Exception as e:
        logger.error(f"Error removing proxy from file: {e}")

if __name__ == '__main__':
    asyncio.run(main())
