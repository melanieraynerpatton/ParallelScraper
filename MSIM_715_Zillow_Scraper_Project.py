from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import time
from time import sleep
from random import randint
from multiprocessing import Process, Pool
import dask.dataframe as ddf
import pandas as pd

#create driver
def create_driver():
    headers = [{'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'},
               {'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'},
               {'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}]
    sec_ch_ua = '\"Chrome\"; v=\"73\", \"(Not;Browser\"; v=\"12\"'

    chromedriver_path = "C:\\Users\\melac\\Desktop\\chromedriver-win64\\chromedriver.exe"

    service = Service(chromedriver_path)
    chrome_options = webdriver.ChromeOptions()
    chrome_options.headless = True
    chrome_options.add_argument("--incognito") #incognito browsing
    chrome_options.add_argument('log-level=3')
    chrome_options.add_argument(f'user-agent={headers[randint(0,2)]}')
    chrome_options.add_experimental_option("detach", True) #keep window open
    chrome_options.add_argument(f'--sec-ch-ua={sec_ch_ua}')
    
    driver = webdriver.Chrome(executable_path=chromedriver_path, options=chrome_options)
    action = ActionChains(driver)

    return driver, action

#find home info
def find_homes(driver, zip_code):
    #prices
    price = driver.find_elements(By.CLASS_NAME, "homecardV2Price")
    prices = list(map(lambda p : int(p.text.replace('$','').replace(',','').replace('+','')), price))

    #current zip
    zips = [zip_code]*len(prices)

    #home info
    info = driver.find_elements(By.CLASS_NAME, "HomeStatsV2.font-size-small")
    infos = list(map(lambda i : i.text, info))
    beds = list(int(line.split('\n')[0].replace(' beds','').replace('—beds','0').replace(' bed','')) for line in infos)
    baths = list(float(line.split('\n')[1].replace(' baths','').replace('—baths','0').replace(' bath','')) for line in infos)
    sqft =list(float(line.split('\n')[2].replace(' sq ft','').replace(' acre (lot)','').replace(' acres (lot)','').replace(' (lot)','').replace('—sq ft','0').replace(',','')) for line in infos)

    all = list(zip(zips, prices, beds, baths, sqft))

    return all

def all_pages(url): 
    try:    
        all_pages = []
    
        driver, action = create_driver()

        driver.get(url)

        zip_code = url[(len(url)-5):]
        try:
            while EC.presence_of_element_located((By.CLASS_NAME, "clickable.buttonControl.button-text")):
                    page = find_homes(driver, zip_code)
                    all_pages.append(page)

                    try:
                        next = driver.find_element(By.XPATH, "/html/body/div[1]/div[8]/div[2]/div[2]/div[5]/div/div[3]/button[2]")
                        action.move_to_element(next).perform()
                        next.click()

                    except:
                        driver.close()
                        flatten(all_pages, zip_code)
                        break
         
        except:
            flatten(all_pages, zip_code)   
            driver.close()
            pass

    except:
        pass


def flatten(output, zip):
    #take lists out of lists
    flattened = [item for houses in output for item in houses]

    #create df
    df = pd.DataFrame(flattened, columns=['zip_code','price','beds','baths','sqft'])

    df = df[(df['price']>0)&(df['beds']>0)&(df['baths']>0)]

    file = f'C:\\Users\\melac\\Desktop\\ODU M.S. DA_DS\\MSIM 715 Home Data\\home_data_{zip}_.csv'
    df.to_csv(file, index=False)
    
def concat_zips(zip_list):
    new_file = 'C:\\Users\\melac\\Desktop\\ODU M.S. DA_DS\\MSIM 715 Home Data\\All Data\\home_data_all_.csv'

    existing_df = pd.DataFrame(columns=['zip_code','price','beds','baths','sqft'])
    existing_df = ddf.from_pandas(existing_df, npartitions=1)

    for i in range(len(zip_list)):

        zips = zip_list[i]
        zip_code = zips[(len(zips)-5):]
        print(i)
        try:
            file = 'C:\\Users\\melac\\Desktop\\ODU M.S. DA_DS\\MSIM 715 Home Data\\home_data_' + zip_code +'_.csv'

            df = pd.read_csv(file)

            existing_df = ddf.concat([existing_df, df])
        except: pass
        


    existing_df.compute().to_csv(new_file, index=False)




if __name__ == "__main__":
    zip_list = ['https://www.redfin.com/zipcode/' + line.rstrip() for line in open(r'C:\Users\melac\source\repos\MSIM 715 Zillow Scraper Project\zipcodes.txt', "r").readlines()]

    start = time.perf_counter()

    for i in range(0, 300, 300):
        sliced_list = zip_list[i:i+300]
        p = Pool(processes=12) 
        p.imap_unordered(all_pages, sliced_list, chunksize=(300//12))
        p.close()
        p.join()
        
    #    print(f'begin concatenation; index {i}')
    pr = Process(target=concat_zips, args=(sliced_list,))
    pr.start()
    pr.join()

    end = time.perf_counter()
    total = end-start
    print(f"Execution time: {round(total//60)} minutes and {round(total-((total//60)*60), 4):0f} seconds")
    


