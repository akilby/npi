from selenium import webdriver
import chromedriver_binary
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys

options = Options()
options.add_argument('--headless')
driver = webdriver.Chrome(chrome_options=options)

url = 'https://www.idfpr.com/applications/professionprofile/default.aspx'
driver.get(url)

ln = 'MainContent_MainContentContainer_LastName'
lnbox = driver.find_element_by_id(ln)
lnbox.send_keys('fred')

submit = driver.find_element_by_id('MainContent_MainContentContainer_Search')
submit.click()

ayys = driver.find_elements_by_css_selector('a[href]')
ayys[16].click()

ayys = driver.find_elements_by_css_selector('a[href]')
for i, a in enumerate(ayys):
    if a.text == "Education":
        educ_no = i

ayys[21].click()

e = 'MainContent_MainContentContainer_repSection2_ctl00_0_SchoolLocation_0'
education = driver.find_element_by_id(e).text

