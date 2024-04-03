import asyncio

import aiohttp
from PIL import ImageFile


async def get_image_dimensions(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.content.read(1024)
                parser = ImageFile.Parser()
                parser.feed(content)
                if parser.image:
                    return parser.image.size
            else:
                return None, None


async def main():
    url = "https://data.sanitino.eu/PRODUCT-45548/f09ec09abcd7a97936ccbb0d?size=feed-1080"
    width, height = await get_image_dimensions(url)
    if width is not None and height is not None:
        print(f"The dimensions of the image are {width}x{height} pixels")
    else:
        print("Failed to fetch image dimensions")


if __name__ == "__main__":
    asyncio.run(main())
