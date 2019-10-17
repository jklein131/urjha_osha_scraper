# use this file to import the incident meta into the database. (no incident data yet)

from bs4 import BeautifulSoup
from lxml import etree
import random, time,os
import requests
import pymongo
from fake_useragent import UserAgent

import pprint

pp = pprint.PrettyPrinter(indent=4)


def pprint(*args):
    pp.pprint(*args)

# data is public domain and can be redistruted
# https://www.dol.gov/general/aboutdol/copyright

url = "https://www.osha.gov/pls/imis/"

ua = UserAgent()

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

mydb = myclient['osha']
mycol = mydb["incidents"]

def stringify_children(node):
    from itertools import chain
    parts = ([node.text] +
            list(chain(*([c.text, str(c), c.tail] for c in node.getchildren()))) +
            [node.tail])
    # filter removes possible Nones in texts and tails
    return ''.join(filter(None, parts))

for event, element in etree.iterparse("all_incidents.html", tag="tr", html=True, recover=True):

    # is the url's in the text
    r = list([x.get('href') for x in element.iter('a')])
    if len(r) == 0:
         #skip the first one
        continue
    just_the_id = r[0].split("=")[1]
    # index 0 -- seq id (useless)
    # index 1 -- incident_id (used for lookup)
    # index 3 -- incident date
    # index 4 -- fatality yes or non-breaking-space
    # index 5 -- incident short description
    # index 6 -- SIC if applicable

    seq_id = ""
    inc_date = ""
    incident_fat = False
    short_desc = ""
    sec_id = "" #if applicable
    td_count = 0
    for x in element.iter():
        if x.tag == "td":
            if td_count == 0:
                #checkbox
                print("checkbox", x.text)
            elif td_count == 1:
                #seq_number
                print("seq", x.text)
                seq_id = x.text
            elif td_count == 2:
                #summeryNumber
                #Throw this away since we will extract from the link (given that sometimes this is the wrong value)
                print("incident_summery_num", x.text)
            elif td_count == 3:
                #eventDate
                print("incident_date", x.text)
                inc_date = x.text
            elif td_count == 4:
                #ReportID
                print("incident_id", x.text)
                #think this is the money value of the report or something
            elif td_count == 5:
                #fatality
                print("incident_fat", x.text)
                if x.text.lower().strip() == "x":
                    incident_fat = True
            elif td_count == 6:
                #SIC
                print("incident_sic", x.text)
                sec_id = ""
            elif td_count == 7:
                #Desc
                print("incident_desc", x.text)
                short_desc = x.text
            td_count+=1
    element.clear()

    if os.path.isfile('data/'+just_the_id+".html"):
        #already imported
        print('doing' + str(just_the_id))
        data = {}
        data["employees"] = {}

        fpath = 'data/' + just_the_id + ".html"
        # try to save in mongo
        header = ''
        header_count = 0
        header_row_1_count = 0
        employee = 0
        employee2 = 0

        for event, element in etree.iterparse(fpath, tag="table", html=True, recover=True):
            rows = [x for x in element.iter('tr')]

        row_count = 0
        tmp = 0
        found_ins_data = False
        for r in rows:
            cols = [x for x in r.iter(['th','td'])]
            col_count = 0

            for c in cols:
                if c.tag == 'td':
                    #extract data here
                    text_parts = [x.text for x in c.iter()]
                    if header == '' and len([x for x in text_parts if x != None]) == 1:
                        #found header:
                        header =[x for x in text_parts if x != None][0]
                        continue
                    print(text_parts, header_count, col_count, row_count)

                    if header_count == 1:
                        if col_count == 0 :
                            if c.get('colspan') != "8":
                                if employee == 0:
                                    employee = 1
                                else:
                                    employee += 1
                                data["employees"]["employee_" + str(employee)] = {}
                                data["employees"]["employee_" + str(employee)]["data"] = {}
                                data["employees"]["employee_" + str(employee)]["detail"] = {}
                                data["employees"]["employee_" + str(employee)]["data"]["inspection"] = [x for x in text_parts if x != None][0]
                        if col_count == 1:
                            data["employees"]["employee_" + str(employee)]["data"]["open_date"] = [x for x in text_parts if x != None][0]
                        if col_count ==2:
                            data["employees"]["employee_" + str(employee)]["data"]["sic"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                        if col_count == 3 :
                            data["employees"]["employee_" + str(employee)]["data"]["company_name"] = [x for x in text_parts if x != None][0]
                        if col_count == 0 and found_ins_data:
                            #keywords are hiding in a div wtf c.iter()
                            if c.get('colspan') == "8":
                                data["keywords"] = [x.strip() for x in str([x for x in c.find('div').itertext()][1]).strip().split(',')]
                        if col_count == 0 :
                            if c.get('colspan') == "8" and not found_ins_data:
                                data["ins_data"] = [x for x in text_parts if x != None][0]
                                found_ins_data = True

                    if header_count == 2:
                        if col_count == 0 :
                            data["end_use"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                        if col_count == 1:
                            data["project_type"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                        if col_count == 2:
                            data["project_cost"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                        if col_count == 3 :
                            data["project_stories"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                        if col_count == 4:
                            data["non_building_ht"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                        if col_count == 5 :
                            data["fatality"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                    if header_count == 3:
                        if col_count == 0:
                            if employee2 == 0:
                                employee2 = 1
                            else:
                                employee2+=1
                            data["employees"]["employee_" + str(employee2)]["detail"]["employee_number"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                        if col_count == 1:
                            data["employees"]["employee_" + str(employee2)]["detail"]["inspection_2"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                        if col_count == 2:
                            data["employees"]["employee_" + str(employee2)]["detail"]["age"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                        if col_count == 3:
                            data["employees"]["employee_" + str(employee2)]["detail"]["sex"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                        if col_count == 4:
                            data["employees"]["employee_" + str(employee2)]["detail"]["degree"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                        if col_count == 5:
                            data["employees"]["employee_" + str(employee2)]["detail"]["nature"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                        if col_count == 6:
                            data["employees"]["employee_" + str(employee2)]["detail"]["ocupation"] = [x for x in text_parts if x != None][0] if len([x for x in text_parts if x != None]) > 0 else ''
                        if col_count == 7:
                            data["employees"]["employee_" + str(employee2)]["detail"]["construction"] = dict((p,q.strip(':').strip()) for p,q in zip([x for x in c.itertext() if ':' not in x], [x for x in c.itertext() if ':' in x]))

                    col_count += 1


                if c.tag == 'th':
                    print("HEADER", c.text)
                    if c.text == 'Open Date':
                        print('ins')
                        header_count = 1
                        header_row_1_count += 1
                    if c.text == "End Use":
                        header_count += 1
                        print('ins')
                    if c.text == "Employee #":
                        header_count += 1
                        print('ins')
            row_count += 1

        myquery = {"incident_id": just_the_id}




        mydoc = mycol.find_one(myquery)

        if mydoc == None:
            print("importing into mongo: "+just_the_id)
            pprint(data)
        if len(rows) != 9:
            print("whoops don't know how to parse " + just_the_id)
            #exit()

        continue


    scrape_url = url+ r[0]
    res = requests.get(
        scrape_url,
        headers={'User-Agent': ua.random}
        )
    if res.status_code > 200:
        print("error")

    with open('data/' +just_the_id+".html","wb") as file:
        file.write(res.content)

    time.sleep(random.randint(2,3))
    print("imported "+just_the_id)



