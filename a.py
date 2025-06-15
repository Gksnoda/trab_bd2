import asyncio
import aiohttp

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def main():
    url = 'http://httpbin.org/get'
    tasks = []
    n_requests = 10000  # tente aumentar até dar erro

    async with aiohttp.ClientSession() as session:
        for _ in range(n_requests):
            task = asyncio.create_task(fetch(session, url))
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        print(f"Total de respostas: {len(results)}")

asyncio.run(main())