import lxml.html
from lxml import etree
from os import listdir
from io import StringIO

def p(a):
    print(etree.tostring(a))

def parse(fil):
    html = lxml.html.parse(fil)

    table = html.xpath('//*[@id="maincontain"]/div/div/table')
    values = table[0].xpath('//tr/td')
    headers = table[0].xpath('//tr/th/text()')
    
    data = {}

    vi = 1
    hi = 0
    while True:
        if hi >= len(headers):
            return data
        while True:
            if vi >= len(values):
                return data
            text = str(etree.tostring(values[vi]))
            if '</strong>' in text:
                vi+=1
                continue
            if '</a>' in text:
                v = values[vi].xpath('a/text()')
            else:
                v = values[vi].xpath('text()')

            if (type(v) == type([]) and len(v) > 1):
                vi += 1
            else:
                if type(v) == type([]):
                    if len(v) == 0:
                        v = 'N/A'
                    else:
                        v = str(v[0])
                break
        
        h = str(headers[hi])

        if 'p.m.' in v or 'a.m.' in v or '\n' in v:
            h = "description"
            hi-=1

        h = h.lower().strip()
        data[h] = v

        hi += 1
        vi += 1


files = listdir('data')

all = []
for fil in files[:100]:
    d = parse('data/'+fil)
    all.append(d)

import json
with open('extracted.json', 'w') as f:
    json.dump(all, f, indent=4)