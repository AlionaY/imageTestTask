import gspread
from oauth2client.service_account import ServiceAccountCredentials

import asyncio

import aiohttp
from PIL import ImageFile


async def fetch_urls_from_sheet(sheet_name, batch_size):
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(credentials)

    sheet = gc.open(sheet_name).sheet1
    urls = sheet.col_values(1)[1:]

    batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
    return batches


async def get_image_dimensions(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            content = await response.content.read(1024)
            parser = ImageFile.Parser()
            parser.feed(content)
            if parser.image:
                return parser.image.size
        else:
            return None, None


async def process_batch(session, batch):
    tasks = []
    for url in batch:
        task = asyncio.create_task(get_image_dimensions(session, url))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results


async def main():
    sheet_name = "Image Parser Test Task"
    batch_size = 100

    batches = await fetch_urls_from_sheet(sheet_name, batch_size)

    async with aiohttp.ClientSession() as session:
        for batch in batches:
            results = await process_batch(session, batch)
            print(results)


if __name__ == "__main__":
    asyncio.run(main())
