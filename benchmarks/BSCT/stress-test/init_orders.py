import asyncio
import json
import logging
import os

import aiohttp

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                    datefmt='%I:%M:%S')
logger = logging.getLogger(__name__)

NUMBER_0F_ITEMS = 100_000
ITEM_STARTING_STOCK = 1_000_000
ITEM_PRICE = 1
NUMBER_OF_USERS = 100_000
USER_STARTING_CREDIT = 1_000_000
NUMBER_OF_ORDERS = 100_000
BATCH_SIZE = 5_000


with open(os.path.join('..', 'urls.json')) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']
    PAYMENT_URL = urls['PAYMENT_URL']
    STOCK_URL = urls['STOCK_URL']


async def populate_databases():
    async with aiohttp.ClientSession() as session:
        async def post_json(url: str) -> dict:
            async with session.post(url) as resp:
                body = await resp.text()
                if resp.status < 200 or resp.status >= 300:
                    raise RuntimeError(f"{url} failed with {resp.status}: {body[:500]}")
                try:
                    return json.loads(body)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"{url} returned non-JSON body: {body[:500]}") from exc

        async def seed_in_chunks(name: str, base_url: str, total: int):
            logger.info("Batch creating %s in chunks of %s ...", name, BATCH_SIZE)
            for start_id in range(0, total, BATCH_SIZE):
                batch_size = min(BATCH_SIZE, total - start_id)
                payload = await post_json(f"{base_url}&start_id={start_id}")
                logger.info(
                    "%s seeded: start=%s count=%s",
                    name,
                    payload.get("start_id", start_id),
                    payload.get("count", batch_size),
                )
            logger.info("%s created", name.capitalize())

        await seed_in_chunks(
            "users",
            f"{PAYMENT_URL}/payment/batch_init/{NUMBER_OF_USERS}/{USER_STARTING_CREDIT}?chunked=true",
            NUMBER_OF_USERS,
        )
        await seed_in_chunks(
            "items",
            f"{STOCK_URL}/stock/batch_init/{NUMBER_0F_ITEMS}/{ITEM_STARTING_STOCK}/{ITEM_PRICE}?chunked=true",
            NUMBER_0F_ITEMS,
        )
        await seed_in_chunks(
            "orders",
            f"{ORDER_URL}/orders/batch_init/{NUMBER_OF_ORDERS}/{NUMBER_0F_ITEMS}/{NUMBER_OF_USERS}/{ITEM_PRICE}?chunked=true",
            NUMBER_OF_ORDERS,
        )


if __name__ == "__main__":
    asyncio.run(populate_databases())
