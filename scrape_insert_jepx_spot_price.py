import os
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import csv
import config

# This tool will download spot summary data from JEPX website and insert record to the table of sqlite.
# config file will be used to specify the path of downloaded file and database file.
# Set up Selenium WebDriver
service = Service()
options = Options()
prefs = {
    "download.default_directory": config.DATA_DIR
}
options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(service=service, options=options)

# List of URLs to download CSV files from
url = "https://www.jepx.jp/electricpower/market-data/spot/"

def fiscal_year():
    year = int(time.strftime("%Y"))
    month = int(time.strftime("%m"))
    if month <= 3:
        return year-1
    else:
        return year
    
def current_date():
    return time.strftime("%Y/%m/%d")

def timestamp():
    return time.strftime("%Y%m%d%H%M%S")

# logging to same file for each fiscal year
def log_download_status(status,url):
    log_entry = f"{status},{timestamp()},{url}\n"
    log_file = f"download_log_{fiscal_year()}.log"
    with open(log_file,"a") as logging:
        logging.write(log_entry)

# this is supposed to process by better selector than absolute Xpath.
#def select_fiscal_year(url):
#    driver.get(url)
#    year_select = Select(driver.find_element(By.XPATH,"//*[contains(@id,'dl-select--spot_summary')]"))
#    year_select.select_by_value(f"spot_summary_{fiscal_year()}.csv")

def download_csv(url):
    try:
        driver.get(url)
        download_button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.XPATH,"/html/body/div[2]/div[1]/div[2]/div/section[6]/ul/li[3]/button"))
        )
        download_button.click()

        select_year = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.XPATH,"/html/body/div[2]/div[2]/div/form/select/option[3]"))
        )
        select_year.click()

        download_button2 = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.XPATH,"/html/body/div[2]/div[2]/div/form/button"))
        )
        download_button2.click()

        log_download_status("success", url)
    except Exception as e:
        print(f"Error during download: {e}")
        log_download_status("error", url)
    time.sleep(10)

download_csv(url)
print(f"Finished loading: {url}")

# Close the WebDriver
driver.quit()

def import_csv_to_sqlite(csv_file_path, db_file_path, table_name):
    conn = sqlite3.connect(db_file_path)

    columns_str = ','.join(columns)
    placeholders = ','.join(['?'] * len(columns))
    update_str = ','.join([f"{col} = excluded.{col}" for col in columns])

    cursor = conn.cursor()
    query = f"""INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})
                ON CONFLICT(delivery_date, interval) DO UPDATE SET
                    {update_str}
            """
    
    with open(csv_file_path, 'r') as file:
        reader = csv.reader(file)
        for _ in range(3):
            next(reader)
        for row in reader:
            cursor.execute(query, row)
    
    conn.commit()
    
    cursor.execute(f"SELECT * FROM {table_name} WHERE {columns[0]} = '{current_date()}'")
    records = cursor.fetchall()

    for record in records:
        print(record)
    conn.close()

    #Rename the file after import
    new_file_name = f"{os.path.splitext(os.path.basename(csv_file_path))[0]}_{timestamp()}.csv"
    new_file_path = os.path.join(os.path.dirname(csv_file_path), new_file_name)
    os.rename(csv_file_path, new_file_path)
    print(f"Renamed {csv_file_path} to {new_file_path}")

    #Move the renamed file to the archive folder
    archive_folder = os.path.join(os.path.dirname(csv_file_path), "archive")
    if not os.path.exists(archive_folder):
        os.makedirs(archive_folder)
    archived_file_path = os.path.join(archive_folder, new_file_name)
    os.rename(new_file_path, archived_file_path)
    print(f"Moved {new_file_path} to {archived_file_path}")

csv_file_path = os.path.join(config.DATA_DIR, f"spot_summary_{fiscal_year()}.csv")
db_file_path = config.DB_PATH
table_name = 'spot_summary'
columns = ('delivery_date','interval','sell_bid_amount','buy_bid_amount','total_contract_amount','system_price','area_price_01','area_price_02',
            'area_price_03','area_price_04','area_price_05','area_price_06','area_price_07','area_price_08','area_price_09',
            'sell_block_total_bid_amount','sell_block_total_contract_amount','buy_block_total_bid_amount','buy_block_total_contract_amount')

import_csv_to_sqlite(csv_file_path, db_file_path, table_name)

print("CSV data has been imported into the SQLite database and displayed on the terminal.")

