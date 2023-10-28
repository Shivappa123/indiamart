from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import boto3
from pymongo import MongoClient
from time import sleep
from bs4 import BeautifulSoup
import wget
import secrets
import string
import os
import cv2

from datetime import datetime
import pandas as pd
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import json
from slack_bot import Slack_Bot

import logging

class IndiaMart():
    def __init__(self) -> None:
        with open('config_xpath.json','r') as config_xpathfile:
            self.config_xpath = json.load(config_xpathfile)
        self.slack_bot = Slack_Bot()
        ACCESS_KEY = self.config_xpath["ACCESS_KEYEle"]
        SECRET_ACCESS_KEY = self.config_xpath["SECRET_ACCESS_KEYEle"]
        options = Options()
        options.headless = False
        options.add_argument(f"--user-data-dir=cookiesfile")
        self.driver = webdriver.Chrome(options=options)
        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_ACCESS_KEY,
        )
        self.s3 = session.resource('s3')
        self.screenshot_path = './ss_path/'
        self.snap_path = './prod_snaps/'
       
        self.cloudFrontUrl = 'https://d3egg8lubiebqb.cloudfront.net/'
       
        client = MongoClient('127.0.0.1', 27017)
        db = client['URL']
        self.company_coll = db['IM-company']
        self.coll = db['IM-product_test1']
        self.logsColl = db['Indiamart_Logs']
       

        self.date = datetime.now().isoformat()
       
        self.logger = logging.getLogger('my_logger')
        self.logger.setLevel(logging.DEBUG)
        log_name = self.date +'_IndiaMart_.log'
        IndiaMartlog_file = os.path.join(log_name)
        file_handler = logging.FileHandler(IndiaMartlog_file)                                      
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.count = 0

        self.client_prod = MongoClient(self.config_xpath["QA_Database_Path"])
        self.QA_db_db = self.client_prod['QA']
        self.QA_touts_coll = self.QA_db_db['touts']
        self.QA_toutswatchlist_coll = self.QA_db_db['toutswatchlist']
        self.QA_toutsDeleted = self.QA_db_db['toutsDeleted']
        self.QA_irrelevantTouts = self.QA_db_db['irrelevantTouts']
        self.TOUTS_db_db = self.client_prod['TOUTS']
        self.TOUTS_url_coll = self.TOUTS_db_db['url']
        self.indiamartUrl = "https://dir.indiamart.com/"
        self.csv_file_path = 'indiamart_key.csv'
       
        self.do_count = list(self.QA_touts_coll.find({"dataFrom":"Indiamart"}))
        print(len(self.do_count))
        
    def load_url(self, url: str,):
        self.driver.get(url)
        
        sleep(5)

    def get_title(self):
        try:
            titleEle = self.driver.find_element('xpath',self.config_xpath["title"]) 
            title = titleEle.text
            return title
        except Exception as e:
                self.logger.error(f"error while searching product_title :{e}")

    def get_company_title(self):
        try:
            titleEle = self.driver.find_element('xpath',self.config_xpath["companyTitle"]) 
            title = titleEle.text
            return title
        except Exception as e:
                self.logger.error(f"error while searching  company_title :{e}")

    def get_company_name(self):
        try:

            cmpEle = self.driver.find_element('xpath',self.config_xpath["compEle"])
            company = cmpEle.text
            return company
        except Exception as e:
                self.logger.error(f"error while searching  companyName :{e}")

    def get_address(self):
        try:
            try:
                addEle = self.driver.find_element('xpath', self.config_xpath["addressEle"]) 
                address = addEle.text
            except:
                address = ""
            return address
        except Exception as e:
                self.logger.error(f"error while searching  address:{e}")
    
    def get_product_description(self):
        try:
            try:
                
                descEle = self.driver.find_element('xpath',self.config_xpath["descripEle_one"]) 
                desc = descEle.text
            except:
                desc = ""
            return desc
        except Exception as e:
                self.logger.error(f"error while searching  productDescription:{e}")

    def get_company_description(self):
        try:
            descEle = self.driver.find_element('xpath',self.config_xpath["cmpDesc"]) 
            desc = descEle.text
            return desc
        except Exception as e:
                self.logger.error(f"error while searching  companyDescription:{e}")
    
    def get_contact(self):
        try:

            try:
                contactEle = self.driver.find_element('xpath', self.config_xpath["contactEle"]).get_attribute('outerHTML')  
                soup = BeautifulSoup(contactEle, 'html.parser')
                divEle = soup.find('div')
                contact = divEle.get('data-number')
            except:
                contact = ""
            return contact
        except Exception as e:
                self.logger.error(f"error while searching  contact :{e}")


    def get_screenshots(self):
        try:
            screenshotList = self.scrape_screenshots()
            s3Links = []
            count = 1
            for ss in screenshotList:
                img_id = ''.join(secrets.choice(string.ascii_letters + string.digits) for x in range(7))
                fileName = self.screenshot_path + str(img_id) + '.png'
                wget.download(ss, fileName)
                count = count + 1
                ssS3Link = self.upload_to_ss_s3(fileName)
                s3Links.append(ssS3Link)
            return s3Links
        except Exception as e:
            self.logger.error(f"unable upload screenshot in s3.Exception: {e}")

    
        
    def upload_to_ss_s3(self, filePath: str):
        try:
            fileName = filePath.split('/')[-1]
            key_name = 'url/photos/' + fileName
            self.s3.meta.client.upload_file(Filename=filePath, Bucket='sarvagya-images', Key=key_name, ExtraArgs={'ContentType': "image/png"})
            s3Path = self.cloudFrontUrl + key_name
            return s3Path
        except Exception as e:
                self.logger.error(f"error while uploading screenshot :{e}")



    def scrape_screenshots(self):
        try:
             
            imagEle = self.driver.find_elements('xpath', self.config_xpath["imgEle"])
            ssList = []
            for ele in imagEle:
                src = ele.get_attribute('src')
                print(src)
                ssList.append(src)
            return ssList
        except Exception as e:
                self.logger.error(f"error while searching  img_url :{e}")

    def save_png(self, new_img, save_path):
        try:
             
            """
                write image in the given image path
            """
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            cv2.imwrite(save_path,new_img)
        except Exception as e:
                self.logger.error(f"error while saving img  :{e}")

    def crop_img(self, img_path):
            try:
                """
                    To crop the screenshot of the profile page
                """
                img=cv2.imread(img_path)
                img_copy = img[:-80, :-20]
                img_copy = cv2.cvtColor(img_copy, cv2.COLOR_BGR2GRAY)
                blured_img = cv2.GaussianBlur(img_copy, (5, 5), 0)
                img_edges = cv2.Canny(blured_img, 100, 200)
                x, y, w, h = cv2.boundingRect(img_edges)
                cropped_img = img[y:y + h, x:x + w]
                return cropped_img
            except Exception as e:
                    self.logger.error(f"error while croping_img :{e}")

    def upload_snap_to_s3(self):
        try:
            img_id = ''.join(secrets.choice(string.ascii_letters + string.digits) for x in range(7))
            save_location = f"{self.snap_path}{img_id}.png"
            height = 2000
            width = 1000
            self.driver.set_window_size(width, height)
            self.driver.save_screenshot(save_location)
            sleep(2)
            cropImg = self.crop_img(save_location)
            self.save_png(cropImg, save_location)
            key_name = 'url/snaps/' + img_id + '.png'
            self.s3.meta.client.upload_file(Filename=save_location, Bucket='sarvagya-images', Key=key_name, ExtraArgs={'ContentType': "image/png"})
            s3Path = self.cloudFrontUrl + key_name
            return s3Path
        except Exception as e:
                self.logger.error(f"error while uploading  snap :{e}")

    def get_company_link(self):
        try:
            cmpEle = self.driver.find_element('xpath', self.config_xpath["company"])
            cmpLink = cmpEle.text
            return cmpLink
        except Exception as e:
                self.logger.error(f"error while searching  company_link:{e}")
    

    def create_log_info(self, key: str, keyword_results: list, qa_ingested: list):
        logInfo = {'searchKeyword': key, 'keywordResults' : keyword_results, 'resultsCount' : len(keyword_results), 'qaIngested': qa_ingested, 'qaCount': len(qa_ingested), 'dateTime' : datetime.now().isoformat()}
        self.logsColl.insert_one(logInfo)




    def close_popup(self):
        try:
            popup_element = self.driver.find_element('xpath',self.config_xpath["popupEle"])  
            if popup_element.is_displayed():
                popup_element.find_element('xpath',self.config_xpath["popupEle_second"]).click()  
        except NoSuchElementException:
            self.logger.error(f"error while searching  popup element")

    def scroll_down_page(self):
        while True:
            try:
                y = 1000
                for timer in range(0, 10):
                    self.driver.execute_script("window.scrollTo(0, " + str(y) + ")")
                    y += 400
                sleep(5)

                next_button = self.driver.find_element('xpath', self.config_xpath["showmore_Results"])
                next_button.click()
            except Exception as e:
                self.logger.error(f"error while searching it could not   scrolling:{e}")
                break

        element_used = False
        while True:
            try:
                if not element_used:
                    
                    keyEle = self.driver.find_element('xpath', self.config_xpath["passUser_element"])
                    keyEle.send_keys(self.config_xpath["number"])
                    sleep(2)
                    keyEle.send_keys(Keys.ENTER)
                    sleep(2)
                    self.driver.find_element('xpath', self.config_xpath["passxpath"]).click()
                    pass_key = self.driver.find_element('xpath', self.config_xpath["passElement"])
                    pass_key.send_keys(self.config_xpath["password"])
                    pass_key.send_keys(Keys.ENTER)
                    sleep(2)
                    element_used = True

                y = 1000
                for timer in range(0, 10):
                    self.driver.execute_script("window.scrollTo(0, " + str(y) + ")")
                    y += 1000
                sleep(3)

                next_button = self.driver.find_element('xpath', self.config_xpath["showmore_Results"])
                self.driver.execute_script("arguments[0].click();", next_button)
            except NoSuchElementException:
                break
            except Exception as e:
                self.logger.error(f"error while page scrolling : {e}")
               

        self.close_popup() 

    def get_product_list(self, keyword: str):
        try:

            urlList = set()
            irrelevantUrls = set()
            p_elements = self.driver.find_elements('xpath', self.config_xpath["product_element"])

            for element in p_elements:
                product_name = element.text
                product_url = element.get_attribute("href")
                product_url = product_url.split('?')[0]
                if ("Irctc" in product_name) or ("IRCTC" in product_name) or ("Tatkal" in product_name) or ("TATKAL" in product_name) or ("tatkal" in product_name) or ("irctc" in product_name):
                    
                    urlList.add(product_url)
                    # print(product_url)
                else:
                     irrelevantUrls.add(product_url)

            logInfo = {'searchKeyword': keyword, 'relevantResults' : list(urlList), 'relevantCount' : len(urlList), 'irrelevantResults': list(irrelevantUrls), 'irrelevantCount': len(irrelevantUrls), 'dateTime' : datetime.now().isoformat()}
            self.logsColl.insert_one(logInfo)
            return urlList
        except Exception as e:
                self.logger.error(f"error while searching  product_urls :{e}")
    
    def search_and_scrape(self):
        try:
            self.driver.get(self.indiamartUrl)
            self.driver.fullscreen_window()
            sleep(2)
            data = pd.read_csv(self.csv_file_path)
            all_keys = data['keys'].tolist()
            urlCountDict = dict()
            allUrls = set()
            for key in all_keys:
                urlList = set()
                search_url = "https://dir.indiamart.com/search.mp?ss="+key
                self.driver.get(search_url)
                self.scroll_down_page()
                productUrlList = self.get_product_list(key)  
                # exit(0)  
                allUrls.update(productUrlList)       
                qa_ingested_urls = self.get_im_product_data(productUrlList) 
                urlCountDict[key] = len(qa_ingested_urls)
                self.create_log_info(key, list(urlList), list(qa_ingested_urls))
            return urlCountDict, len(allUrls)
        except Exception as e:
                self.logger.error(f"error while searching  scraping_urls :{e}")
               


    def get_company_contacts(self, comapnyUrl: str):
        try:
            url = comapnyUrl + 'enquiry.html'
            self.driver.get(url)
            sleep(5)
            addEle = self.driver.find_element('xpath', self.config_xpath["cmpAddress"]) 
            address = addEle.text
            contactEle = self.driver.find_element('xpath',self.config_xpath["cmpContact"])
            contactNumber = contactEle.text
            return address, contactNumber  
        except Exception as e:
            self.logger.error(f"error while company_contact data:{e}")

    def get_company_data(self, companyUrl: str):
        try:

            dataDict = dict()
            dataDict['link'] = companyUrl
            dataDict['title'] = self.get_company_title()
            dataDict['description'] = self.get_company_description()
            dataDict['companyName'] = dataDict['title']
            dataDict['companyLink'] = companyUrl
            address, contact = self.get_company_contacts(companyUrl)
            contact = contact.split('Call ')[-1]
            dataDict['address'] = address
            dataDict['contact'] = [contact]
            dataDict['screenshots'] = []
            return dataDict
        except Exception as e:
            self.logger.error(f"error while scraping company data: {e}")

    def get_product_data(self, url: str):
        try:
            dataDict = dict()
            dataDict['link'] = url
            dataDict['title'] = self.get_title()
            dataDict['description'] = self.get_product_description()
            dataDict['companyName'] = self.get_company_name()
            dataDict['companyLink'] = self.get_company_link()
            dataDict['address'] = self.get_address()
            dataDict['contact'] = [self.get_contact()]
            dataDict['screenshots'] = self.get_screenshots()
            return dataDict
        except Exception as e:
            self.logger.error(f"error while scraping product data: {e}")

    def get_product_final_data(self, url):
        try:
             
            dataDict = dict()
            self.load_url(url)
            dataDict['Snapshot'] = self.upload_snap_to_s3()
            dataDict.update(self.get_product_data(url))
            dataDict.update(self.commom_dict())
            # print(dataDict)
            return dataDict
        except Exception as e:
                self.logger.error(f"error while loading productUrls :{e}")


    def get_company_final_data(self,url):
        try:
            dataDict = dict()
            self.load_url(url)
            dataDict['Snapshot'] = self.upload_snap_to_s3()
            dataDict.update(self.get_company_data(url))
            dataDict.update(self.commom_dict())
            print(dataDict)
            return dataDict
        except Exception as e:
                self.logger.error(f"error while loading companyUrls:{e}")
    
           

    def add_data_to_db(self, urlList, companyFlag: bool):
        qaIngestedSet = set()
    
        for url in urlList:
            print(url, '   ---- ')
            data1 = self.QA_touts_coll.find_one({'link': url})
            if data1 == None:
                print("The url is not in touts_collection:", url)
                data2 = self.QA_toutswatchlist_coll.find_one({'link': url})
                if data2 == None:
                    print("The url is not in toutswatchlist_collection:", url)
                    data4 = self.QA_toutsDeleted.find_one({'link': url})
                    if data4 == None:
                        print("The url is not in QA_toutsDeleted:", url)
                        data5 = self.QA_irrelevantTouts.find_one({'link': url})
                        if data5 == None:
                            print("The url is not in QA_irrelevantTouts:", url)
                            data6 = self.TOUTS_url_coll.find_one({'link': url})
                            if data6 == None:
                                print("The url is not in TOUTS_ext_coll:", url) 
                                obj = self.coll.find_one({'link' : url})
                                if companyFlag:
                                    try:
                                        dataDict = self.get_company_final_data(url)
                                        dataDict['cmpFlag'] = True
                                        # self.QA_touts_coll.insert_one(dataDict)
                                        self.count = self.count + 1
                                        qaIngestedSet.add(url)
                                    except Exception as e:
                                        self.logger.error(f"An error occurred while getting company data: {e}")
                                    if obj == None:
                                        self.company_coll.insert_one(dataDict)
                                else:
                                    try:
                                        dataDict = self.get_product_final_data(url)
                                        # self.QA_touts_coll.insert_one(dataDict)
                                        self.count = self.count + 1
                                        qaIngestedSet.add(url)
                                        if obj == None:
                                            self.coll.insert_one(dataDict)
                                    except Exception as e:
                                        self.logger.error(f"An error occurred while getting product data: {e}")
                                    
        return qaIngestedSet
        

    def get_im_product_data(self,urlList):
        qaIngestedUrls = self.add_data_to_db(urlList,False)
        return qaIngestedUrls
            
    def find_companydata(self,url):
        self.add_data_to_db([url], True)
    

        
    def commom_dict(self):
        try:
             
            dataDict = dict()
            dataDict['search_key'] = ['Irctc']
            dataDict['indicators'] = []
            dataDict['threatSeverity'] = 'Critical'
            dataDict['threatSource'] = 'URL'
            dataDict['domain_name'] = 'indiamart.com'
            dataDict['dataFrom'] = 'Indiamart'
            dataDict['threatType'] = 'tout'
            dataDict['falsePositive'] = False
            dataDict['resolved'] = False
            dataDict['certificate_whitelisted'] = False
            dataDict['takedown'] = False
            dataDict['deeper_analysis'] = False
            dataDict['researcher_comment'] = ""
            dataDict['dateResolved'] = ''
            dataDict['dateOfGeneration'] = datetime.now().isoformat()
            return dataDict
        except Exception as e:
                    self.logger.error(f"error while creating commom dict :{e}")


    def get_count(self, keyCount, totalCount):
        msg = {
                "dateTime":datetime.now().isoformat(),
                "DataSource":"Indiamart",
                "Company" : "IRCTC",
                "keywords": keyCount,
                "total founds":totalCount,
                "Already existed" :totalCount-self.count,
                "QA Ingested":self.count
                }
        
        print(msg)
        # self.slack_bot.send_message(msg)

if __name__ == "__main__":
    im = IndiaMart() 

    # data = pd.read_csv(r'company_urls.csv')
    # all_url = data['url'].tolist()
    # for url in all_url:
    #     im.find_companydata(url)
    keyCount,totalCount = im.search_and_scrape() 
    
    im.get_count(keyCount, totalCount) 
    im.driver.close()

