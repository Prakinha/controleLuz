"""L530, L535 and L630 Example"""

import asyncio
import os

from tapo import ApiClient
from tapo.requests import Color


async def main():
    tapo_username = os.getenv("TAPO_USERNAME")
    tapo_password = os.getenv("TAPO_PASSWORD")
    ip_address = os.getenv("IP_ADDRESS")
    ip_address1 = "192.168.202.153"
    ip_address2 = "192.168.202.196"
    ip_address3 = "192.168.202.99"
    ip_address4 = "192.168.202.14"

    client = ApiClient(tapo_username, tapo_password)
    device1 = await client.l530(ip_address1)
    device2 = await client.l530(ip_address2)
    device3 = await client.l530(ip_address3)
    device4 = await client.l530(ip_address4)

    print("Turning device on...")
    await device1.on()
    await device2.on()
    await device3.on()
    await device4.on()

    print("Waiting 2 seconds...")
    await asyncio.sleep(2)

    # print("Setting the brightness to 30%...")
    # await device1.set_brightness(30)
    # await device2.set_brightness(30)

    # print("Setting the color to `Chocolate`...")
    await device1.set_color(Color.ForestGreen)
    await device2.set_color(Color.ForestGreen)
    await device3.set_color(Color.ForestGreen)
    await device4.set_color(Color.ForestGreen)
    print("Waiting 2 seconds...")
    await asyncio.sleep(2)

    # print("Setting the color to `Deep Sky Blue` using the `hue` and `saturation`...")
    # await device2.set_hue_saturation(195, 100)
    # await device1.set_hue_saturation(195, 100)

    # print("Waiting 2 seconds...")
    # await asyncio.sleep(2)

    # print("Setting the color to `Incandescent` using the `color temperature`...")
    # await device1.set_color_temperature(2700)
    # await device2.set_color_temperature(2700)

    # print("Waiting 2 seconds...")
    # await asyncio.sleep(2)

    # print("Using the `set` API to set multiple properties in a single request...")
    # await device1.set().brightness(50).color(Color.HotPink).send(device1)
    # await device2.set().brightness(50).color(Color.HotPink).send(device2)

    print("Waiting 2 seconds...")
    await asyncio.sleep(2)

    print("Turning device off...")
    await device1.off()
    await device2.off()
    await device3.off()
    await device4.off()

    device_info = await device1.get_device_info()
    print(f"Device info: {device_info.to_dict()}")

    device_usage = await device2.get_device_usage()
    print(f"Device usage: {device_usage.to_dict()}")


if __name__ == "__main__":
    asyncio.run(main())
