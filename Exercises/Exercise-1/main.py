import aiohttp
import asyncio
import os
import zipfile
import shutil

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


async def check_url_validity(session, url):
    """Check if the URL is valid by sending a Head request asynchronously."""
    try:
        async with session.head(url) as response:
            if response.status == 200:
                return True
            else:
                str_log = f"URL not valid: {url} (Status Code: {response.status})"
                with open("log_file.txt", "a") as f:
                    f.write(str_log + "\n")
                print(str_log)
                return False
    except Exception as e:
        print(f"Error checking URL: {url} - {e}")
        return False


async def download_and_unzip(session, url, file_name, download_dir):
    """Download and unzip files asynchronously."""
    zip_path = f"{download_dir}/{file_name}"

    # Download file asynchronously
    async with session.get(url) as response:
        with open(zip_path, "wb") as f:
            while True:
                chunk = await response.content.read(1024)
                if not chunk:
                    break
                f.write(chunk)

    # Unzip the file
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(download_dir)

    # Delete the zip file after the extraction
    os.remove(zip_path)

    # Remove the __MACOSX directory if it exists
    remove_macosx_dir(download_dir)


def remove_macosx_dir(directory):
    """Remove all __MACOSX directories and their contents recursively."""
    for root, dirs, files in os.walk(directory):
        if "__MACOSX" in dirs:
            macosx_path = os.path.join(root, "__MACOSX")
            print(f"Removing {macosx_path} directory...")
            shutil.rmtree(macosx_path)
            print(f"Removed {macosx_path}")


async def download_files():
    """Main function to orchestrate the asynchronous downloads."""
    # Ensure the 'Downloads' directory exists
    download_dir = "Downloads"
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    list_files = [i.split("/")[-1] for i in download_uris]

    # Create an aiohttp session for making HTTP requests
    async with aiohttp.ClientSession() as session:
        # List of async tasks to check URLs and download files
        tasks = []

        for url, file_name in zip(download_uris, list_files):
            if await check_url_validity(session, url):
                tasks.append(download_and_unzip(session, url, file_name, download_dir))
            else:
                print(f"Skipping download for {file_name} due to invalid URL.")

        # Await all download tasks concurrently
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Use ProactorEventLoop for Windows
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    # Run the asyncio event loop
    asyncio.run(download_files())
