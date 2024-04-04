import logging

import gspread
from aiohttp import ClientError
from oauth2client.service_account import ServiceAccountCredentials
import asyncio
import aiohttp
from PIL import Image
from io import BytesIO
from googleapiclient.discovery import build

SPREADSHEET_ID = "1vHP__2QbwKttfJfI8KkmLvYhgaJ5minKOKscXacz73k"
SPREADSHEET_NAME = "Image Parser Test Task"

dimensions = []


async def fetch_urls_from_sheet(sheet_name, batch_size, credentials):
    gc = gspread.authorize(credentials)

    sheet = gc.open(sheet_name).sheet1
    urls = sheet.col_values(1)[1:]

    batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
    return batches


async def process_batches(batches, result_list):
    async with aiohttp.ClientSession() as session:
        for batch in batches:
            results = await process_batch(session, batch)
            result_list += results


async def process_batch(session, batch):
    tasks = []
    for url in batch:
        task = asyncio.create_task(get_image_dimensions(session, url))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results


async def get_image_dimensions(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.read()
                image = Image.open(BytesIO(content))
                width, height = image.size
                return width, height
            else:
                return None, None
    except ClientError as error:
        logging.error(f'Error fetching image dimensions for URL: {error} - {url}')


def write_data_to_google_sheet(service, dimension_list):
    print('write_data_to_google_sheet')
    value_list = [convertTuple(i) for i in dimension_list]
    value_input_option = 'USER_ENTERED'
    body = {
        'majorDimension': 'COLUMNS',
        'values': [value_list]
    }
    cell_range = 'B2'
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        valueInputOption=value_input_option,
        range=cell_range,
        body=body
    ).execute()


def convertTuple(dimension_tuple):
    try:
        return f'{dimension_tuple[0]}x{dimension_tuple[1]}'
    except TypeError:
        return str(dimension_tuple)


async def main():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    spreadsheet_service = build('sheets', 'v4', credentials=credentials)
    batch_size = 100

    batches = await fetch_urls_from_sheet(SPREADSHEET_NAME, batch_size, credentials)
    await process_batches(batches, dimensions)
    write_data_to_google_sheet(spreadsheet_service, dimensions)


if __name__ == "__main__":
    asyncio.run(main())
