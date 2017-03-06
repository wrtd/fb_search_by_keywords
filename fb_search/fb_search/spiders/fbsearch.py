import scrapy
import json
import MySQLdb
import config
import re

from array import *
from kafka import KafkaProducer, KafkaConsumer
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from scrapy.http import TextResponse
from pyvirtualdisplay import Display
# import pyautogui
# from PIL import Image

import time


class FBS(scrapy.Spider):
    # pdb.set_trace()
    name = "fbs"
    allowed_domains = ["http://www.facebook.com"]
    start_urls = ["http://www.facebook.com"]

    def __init__(self):
        # path_to_chromedriver = 'D://chromedriver'
        # path = 'C:\Program Files\Mozilla Firefox\Firefox'
        # path_to_chromedriver='/usr/local/bin/chromedriver'
        # self.driver = webdriver.Chrome(executable_path=path_to_chromedriver)
        # display = Display(visible=0, size=(800, 600))
        # display.start()
        # self.driver = webdriver.Chrome(executable_path=path_to_chromedriver)
        self.driver = webdriver.Firefox()
        self.driver.set_window_size(1280, 720)
        # self.driver = webdriver.PhantomJS()

    # Login facebook
    def start_requests(self):
        db = MySQLdb.connect(host="192.168.20.39", port=3306, user="root", passwd="rahasia2016", db="twitter_monitor")
        cursor = db.cursor()
        sql = "select keyword from keywords"
        cursor.execute(sql)
        results = cursor.fetchall()

        time.sleep(10)
        try:
            self.driver.get('http://www.facebook.com')

            # Username
            email = self.driver.find_element_by_name('email')
            email.click()
            email.send_keys("najwa2azahra@gmail.com")
            time.sleep(3)

            # Password
            password = self.driver.find_element_by_name('pass')
            password.click()
            password.send_keys("rahasia2016")
            time.sleep(3)
            password.send_keys(Keys.ENTER)
            time.sleep(5)

            # import pdb;pdb.set_trace()
            # Search Button

            for fb in range(len(results)):

                url = results[fb]
                url = str(url).replace('(', '').replace(')', '').replace('\'','').replace(',','')
                time.sleep(2)
                try:
                    search = self.driver.find_element_by_xpath('//input[@placeholder="Search Facebook"]')
                except:
                    search = self.driver.find_element_by_xpath('//input[@placeholder="Find friends"]')
                time.sleep(3)
                search.click()
                time.sleep(5)
                search.send_keys(Keys.DELETE)
                search.send_keys(url)
                time.sleep(2)
                search.send_keys(Keys.ARROW_UP)
                time.sleep(1)
                search.send_keys(Keys.ENTER)
                time.sleep(2)

                # Update
                try:
                    self.driver.find_element_by_xpath("//*[contains(text(),'Latest')]").click()
                except:
                    search.send_keys(Keys.TAB+Keys.TAB+Keys.TAB+Keys.TAB+Keys.TAB+Keys.TAB+Keys.TAB+Keys.TAB+Keys.TAB+Keys.ARROW_RIGHT)
                    search.send_keys(Keys.ENTER)
                time.sleep(5)
                # print "====================++================================"
                # # import pdb;pdb.set_trace()
                # try:
                #     x, y = pyautogui.locateCenterOnScreen('/media/lenovo/CC8C59F38C59D912/tmp/fb_search/fb_search/spiders/update.png')
                #     pyautogui.click(x=x, y=y, clicks=1, interval=0, button='left')
                #     time.sleep(1)
                # except:
                #     try:
                #         x, y = pyautogui.locateCenterOnScreen('/media/lenovo/CC8C59F38C59D912/tmp/fb_search/fb_search/spiders/update1.png')
                #         pyautogui.click(x=x, y=y, clicks=1, interval=0, button='left')
                #         time.sleep(1)
                #     except:
                #         try:
                #             x, y = pyautogui.locateCenterOnScreen('/media/lenovo/CC8C59F38C59D912/tmp/fb_search/fb_search/spiders/update3.png')
                #             pyautogui.click(x=x, y=y, clicks=1, interval=0, button='left')
                #             time.sleep(1)
                #         except:pass
                time.sleep(5)
                print "======================================================"
                file = 0
                # import pdb;pdb.set_trace()
                for i in range(1, 1000):
                    file = file + 1
                    response = TextResponse(self.driver.current_url, body=self.driver.page_source, encoding='utf-8')
                    # import pdb;pdb.set_trace()
                    #get_id
                    try:
                        userid1 = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']//span[@class="fwn fcg"]/span/a/@data-hovercard').extract()
                        userid2 = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']//span[@class="fwn fcg"]/span/span/a/@data-hovercard').extract()
                    except Exception, e:
                        print e
                    try:
                        userid1 = ''.join(userid1)
                        userid1 = re.findall('\d+', userid1)[0]
                        userid1 = ''.join(userid1)
                    except:
                        userid1 = ''
                    try:
                        userid2 = ''.join(userid2)
                        userid2 = re.findall('\d+', userid2)[0]
                        userid2 = ''.join(userid2)
                    except:
                        userid2 = ''
                    if userid2!="":
                        if userid1=="":
                            userid=userid2
                        else:
                            userid = userid2+"_"+userid1
                    else:
                        userid = userid1
                    if userid=="":
                        break
                    #get_name
                    try:
                        akun1 = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/div/div/div[2]/h5/span/span/span/a/text()').extract()
                        akun2 = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/div/div/div[2]/h5/span/span/a/text()').extract()
                        akun3 = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/div/div/div[2]/h5/span/span/a[2]/text()').extract()
                        akun4 = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/div/div/div[2]/h5/span/span/span/a/text()').extract()
                    except Exception, e:
                        print e
                    akun1 = ''.join(akun1).encode('utf-8')
                    akun2 = ''.join(akun2).encode('utf-8')
                    akun3 = ''.join(akun3).encode('utf-8')
                    akun4 = ''.join(akun4).encode('utf-8')
                    # Clean Account
                    akun = [akun1, akun2, akun3, akun4]
                    new_akun = 0
                    data = [0] * 4
                    for a in range(0, 4):
                        if akun[a] != None:
                            # print akun[a]
                            data[new_akun] = akun[a]
                            new_akun = new_akun + 1
                        else:
                            pass
                    bersih = ""
                    for q in range(len(data)):
                        data_baru = data[q]
                        if data_baru == 0:
                            break
                        else:
                            if q == 0:
                                try:
                                    bersih = str(bersih) + str(data[q])
                                except UnicodeDecodeError:
                                    bersih = str(bersih) + str(data[q]).encode('utf-8')
                            else:
                                bersih = str(bersih) + ', ' + str(data[q])

                    # Get Time
                    try:
                        waktu = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']//span[@class="fsm fwn fcg"]/a/abbr/@title').extract_first()
                    except Exception, e:
                        print e
                    # Get Post
                    try:
                        post1 = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']//div[@class="_5pbx userContent"]/p/text()').extract()
                        post2 = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']//div[@class="_5pbx userContent"]/div/p/text()').extract()
                        post3 = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']//div[@class="_5pbx userContent"]/div/div/p/text()').extract()
                    except Exception, e:
                        print e

                    try:
                        idpost = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']/div/@data-bt').extract()
                    except Exception, e:
                        print e
                    idpost = ''.join(idpost).encode('utf-8')
                    post_id = idpost.split(',')[0]
                    post_id = post_id.split(':')[1]
                    try:
                        from BeautifulSoup import BeautifulSoup
                    except ImportError:
                        from bs4 import BeautifulSoup
                    time.sleep(2)

                    str1 = ''.join(post1).replace("  ", url)
                    str2 = ''.join(post2).replace("Continue Reading", "")
                    str3 = ''.join(post3).replace("  ", url)
                    cleantext = BeautifulSoup(str2).text
                    try:
                        posting = str1.encode('utf-8') + cleantext.encode('utf-8') + str3.encode('utf-8')
                    except Exception, e:
                        print e

                    #get_hashtag
                    try:
                        hashtag = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']//a[@class="_58cn"]')
                    except Exception, e:
                        print e

                    # Get Artikel
                    try:
                        link = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']//div[@class="mtm"]/div/div/div/span/div[2]/div/div/a/@href').extract_first()
                    except Exception, e:
                        print e
                    # Get Like
                    try:
                        like = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']//span[@class="_4arz"]/span/text()').extract()
                    except Exception, e:
                        print e
                    # Get Comment
                    try:
                        comment = response.xpath('//div[@id="BrowseResultsContainer"]/div[' + str(i) + ']//a[@aria-live="polite"]/em/text()').extract()
                    except Exception, e:
                        print e

                    # Print All
                    print file
                    print "========================================================================================="
                    print userid
                    print "========================================================================================="
                    print bersih
                    print "========================================================================================="
                    print waktu
                    print "========================================================================================="
                    print post_id
                    print "========================================================================================="
                    print posting
                    print "========================================================================================="
                    if link != None:
                        print "Link artikel = " + link
                    else:
                        print "Tidak ada artikel"
                        time.sleep(2)
                    print "========================================================================================="
                    try:
                        like = ''.join(like)
                    except Exception, e:
                        print e
                    if like == "":
                        like = like.replace("", "0")
                    else:
                        pass
                    print like
                    print "========================================================================================="
                    try:
                        comment = ''.join(comment)
                    except Exception, e:
                        print e
                    if comment == "":
                        comment = comment.replace("", "0")
                    else:
                        pass
                    print comment
                    print "========================================================================================="
                    self.driver.find_element_by_xpath('/html/body').send_keys(Keys.PAGE_DOWN)
                    total = json.dumps({'type': 'latest_post', 'UserId': userid, 'Nama': bersih, 'Waktu': waktu, 'PostId': post_id,
                                        'Posting': posting, 'Link': link, 'Like': like, 'Comment': comment})
                    for kafka in range(0, 20):
                        time.sleep(1)
                        try:
                            # import pdb;pdb.set_trace()
                            producer = KafkaProducer(bootstrap_servers=config.bootstrap_servers)
                            producer.send(config.kafka_topic, total)
                            # print post_id
                            print "======================================="
                            print total
                            print "DONE"
                            print "======================================="
                            break
                        except:
                            print "======================================="
                            print "Fail"
                            print "======================================="
                            pass
                    try:
                        endof = response.xpath('//div[@id="browse_end_of_results_footer"]/div/div/div/text()').extract()
                    except Exception, e:
                        print e
                    endof = ''.join(endof).encode('utf-8')
                    if endof=="End of Results":
                        break
            db.close()
        except Exception, e:
            print e
        db.close()
