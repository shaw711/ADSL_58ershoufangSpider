import requests
from requests import RequestException
import re
from bs4 import BeautifulSoup
import lxml
import pymongo
from multiprocessing import Pool
from requests.exceptions import ConnectionError
from hashlib import md5

MONGO_URL='localhost'
MONGO_DB='深圳'
MONGO_TABLE='second-hand_house'

client = pymongo.MongoClient(MONGO_URL, connect=False)
db = client[MONGO_DB]

headers={
'Host':'sz.58.com',
'Referer':'http://sz.58.com/ershoufang/?PGTID=0d00000c-0000-0f83-3140-b05613b78a21&ClickID=1',
'Connection': 'keep-alive',
'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
'Accept-Encoding': 'gzip, deflate',
'Accept-Language': 'zh-CN,zh;q=0.9',
'Cookie': 'id58=c5/njVoZVhZgE3CkFLeEAg==; als=0; commontopbar_myfeet_tooltip=end; city=nj; 58home=nj; commontopbar_ipcity=nj%7C%E5%8D%97%E4%BA%AC%7C0; ppStore_fingerprint=6438FADD1DD2B99631ED9E34A69D559F0BD2915FEFE7705C%EF%BC%BF1512788860416; commontopbar_new_city_info=4%7C%E6%B7%B1%E5%9C%B3%7Csz; 58tj_uuid=97a5ba11-e074-4d71-91d8-1df2fc2f70eb; new_session=0; new_uv=11; utm_source=; spm=; init_refer=http%253A%252F%252Fcallback.58.com%252Ffirewall%252Fvalid%252F3657591974.do%253Fnamespace%253Dershoufangphp%2526url%253Dnj.58.com%25252Fershoufang%25252F32030013489979x.shtml; xxzl_deviceid=maf3seSgurPNuJSfTVD3bIB%2BHhlP1Qj11U%2FAKSYugyQ7xb47M7W7dnjtXbAF4T%2BH'
}

proxy_pool_url = 'http://120.27.34.24:8000'
proxy = None

#请求代理页
def get_proxy():
    try:
        response=requests.get(proxy_pool_url)
        if response.status_code==200:
            print(response.text)
            return response.text
        return None
    except ConnectionError:
        return None

max_count=5
#获取代理
def get_html(url,count=1):
    print('Crawling',url)
    print('Trying count',count)
    global proxy
    if count>=max_count:
        print('Tried Too Many Counts')
        return None
    try:
        if proxy:
            proxies={
                'http':'http://'+proxy
            }
            response=requests.get(url,allow_redirects=False,headers=headers,proxies=proxies)
        else:
            response=requests.get(url,allow_redirects=False,headers=headers)
        if response.status_code==200:
            return response.text
        if response.status_code==302:
            print('302')
            proxy=get_proxy()
            if proxy:
                print('Using proxy',proxy)
                return get_html(url)
            else:
                print('Get Proxy Failed')
                return None
    except ConnectionError as e:
        print('Error Occurred',e.args)
        proxy=get_proxy()
        count+=1
        return get_html(url,count)




#获取一页面
def get_main_page(num):
#    url = 'http://nj.58.com/ershoufang/0/pn' + str(num) + '/?PGTID=0d300000-0000-0b45-e43d-53797c7244b6&ClickID=1'
    url = 'http://sz.58.com/ershoufang/0/pn' + str(num) + '/?PGTID=0d00000c-0000-09b4-e6aa-466ec1b61e66&ClickID=1'
#    url='http://nj.58.com/ershoufang/0/pn'+str(num)+'/?from=1-list-0&iuType=p_0&PGTID=0d300000-0000-046c-3bfd-2904eab0c866&ClickID=1'
    text=get_html(url)
    return text

#爬取一页中所有链接
def parse_main_page(text):
    soup=BeautifulSoup(text,'lxml')
    items=soup.select('.house-list-wrap li')
    final_url=[]
    for item in items:
        url_contain=soup.select('.title')
        print(url_contain)
        if url_contain:
            for item in url_contain:
                item=str(item)
 #               print(item)
                guize=re.compile('<h2\sclass.*?<a\shref="(.*?)"\sonclick.*?</h2>',re.S)
                url=re.findall(guize,item)
                print(url)
                final_url=final_url+url
            return(final_url)
        #将url列表合并为同一个返还


#进一步请求展开url
def get_detail_page(url):
    try:
        response=requests.get(url,headers=headers)
        if response.status_code==200:
 #           print(response.text)
            return response.text
        else:
          return None
    except RequestException:
        return None

# 进一步请求展开url
#def get_detail_page(url):
#    text=get_html(url)
#    return text



#获取展开页面detail信息
def parse_detail_page(content):
    if content!=None:
        try:
            soup=BeautifulSoup(content,'lxml')
            for titleItem in soup.select('.house-title .c_333.f20'):
                print(titleItem.get_text())
                title=titleItem.get_text()

            for ownerItem in soup.select('.f14.c_333.jjrsay'):
#                print(ownerItem.get_text()[0:3])
#                owner=ownerItem.get_text()[0:3]
                 print(ownerItem.get_text())
                 owner=ownerItem.get_text()
          #      print(type(owner))

            for priceItem in soup.select('.price'):
                print(priceItem.get_text())
                price=priceItem.get_text()
            for phoneItem in soup.select('.phone-num'):
                print(phoneItem.get_text())
                phonenum=phoneItem.get_text()
            for updateItem in soup.select('.house-update-info'):
                print(updateItem.get_text()[0:6].strip())
                date=updateItem.get_text()[0:6].strip()
          #      print(type(date))
            id=(title+owner+price+phonenum+date).encode("utf-8")
            hash = md5(id).hexdigest()
            return({'title':title,'owner':owner,'price':price,'phonenum':phonenum,'date':date,'hash':hash})
        except ValueError:
            pass



def save_to_mongo(result,value):
    if db[MONGO_DB].update({'hash':value},{'$set':result},True): #对mongodb数据库进行根据title的查重
        print('Saved to Mongo',value)
    else:
        print('Saved to Mongo Failed')


def main(page):
    text=get_main_page(page)
    for item in parse_main_page(text):
        content=get_detail_page(item)
        result=parse_detail_page(content)
        if result:
            save_to_mongo(result,result['hash'])

if __name__=='__main__':

 #   p = Pool(3)
 #   p.map(main, [i * 1 for i in range(1,5)])
 #   main(2)
    for i in range(1, 71):
        main(i)
 #   main(range(1,5))