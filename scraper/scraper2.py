# get lists of new contracts (published after last succesful run)

# note: I started to use xmltodict, but run into probles with insufficient memory
# on the server (needed >0.5G), so switched to local-server
# note 2: "git pull" needs to work without any problem + git needs to be set to ssh (not https)

import csv
import datetime
import git
import json
import os
import requests
import xmltodict

import settings

# repo settings
repo = git.Repo(settings.git_dir)
git_ssh_identity_file = settings.ssh_file
o = repo.remotes.origin
git_ssh_cmd = 'ssh -i %s' % git_ssh_identity_file
o.pull()    #note: requires git pull to run correctly from the

path = settings.git_dir + "data/"


# get last date
with open(path + "log.csv") as fin:
    csvdr = csv.DictReader(fin)
    for row in csvdr:
        if row['success'] == 'True':
            lastmonth = row['month']

existing = {}
with open(path + "data2.csv") as fdata:
    csvdr = csv.DictReader(fdata)
    for r in csvdr:
        existing[r['id']] = r['id']
with open(path + "data2.csv") as fin:
    reader = csv.reader(fin)
    header = next(reader)

def n2(n):
    if n < 10:
        return "0" + str(n)
    else:
        return str(n)

def nextmonth(ym):
    chunks = ym.split('_')
    print(chunks)
    if int(chunks[1]) == 12:
        newym = str(int(chunks[0]) + 1) + '_01'
    else:
        newym = str(chunks[0]) + "_" + n2(int(chunks[1])+1)
    return newym

def name2name(h,obj):
    try:
        if h == 'contract_id':
            return obj['identifikator']['idSmlouvy']
        elif h == 'version_id' or h == 'id':
            return obj['identifikator']['idVerze']
        elif h == 'url':
            return obj['odkaz']
        elif h == 'published':
            return obj['casZverejneni']
        elif h == 'principal:name':
            return obj['smlouva']['subjekt']['nazev']
        elif h == 'principal:id':
            return obj['smlouva']['subjekt']['ico']
        elif h == 'principal:address':
            return obj['smlouva']['subjekt']['adresa']
        elif h == 'principal:ds':
            return obj['smlouva']['subjekt']['datovaSchranka']
        elif h == 'subject':
            return obj['smlouva']['predmet']
        elif h == 'concluded':
            return obj['smlouva']['datumUzavreni']
        elif h == 'number':
            return obj['smlouva']['cisloSmlouvy']
        elif h == 'approved_by':
            return obj['smlouva']['schvalil']
        elif h == 'value':
            return obj['smlouva']['hodnotaVcetneDph']
        elif h == 'value_without_vat':
            return obj['smlouva']['hodnotaBezDph']
        elif h == 'value_currency':
            try:
                return obj['smlouva']['ciziMena']['hodnota']
            except:
                return ''
        elif h == 'currency':
            try:
                return obj['smlouva']['ciziMena']['mena']
            except:
                return ''
        elif h == 'contractors':
            try:
                obj['smlouva']['smluvniStrana'][0]
                return json.dumps(obj['smlouva']['smluvniStrana'])
            except:
                try:
                    return json.dumps([obj['smlouva']['smluvniStrana']])
                except:
                    return json.dumps([])
    except:
        return ''

# get data
actualmonth = datetime.datetime.now().strftime('%Y_%m')
month = nextmonth(lastmonth)
while month <= actualmonth:
    url = "https://data.smlouvy.gov.cz/dump_" + month + ".xml"
    print (url)
    r = requests.get(url)
    r.encoding = "utf-8"
    djson = xmltodict.parse(r.text)

    n = 0
    with open (path + "data2.csv", "a") as fout:
        csvdw = csv.DictWriter(fout, fieldnames=header)
        for item in djson['dump']['zaznam']:
            try:
                existing[item['identifikator']['idVerze']]
            except:
                newitem = {}
                for h in header:
                    newitem[h] = name2name(h,item)
                csvdw.writerow(newitem)
                n += 1
                # raise(Exception)
    with open(path + "log.csv") as fin:
        logreader = csv.reader(fin)
        logheader = next(logreader)
    with open(path + "log.csv", "a") as fin:
        csvdr = csv.DictWriter(fin, fieldnames=logheader)
        if djson['dump']['dokoncenyMesic'] == "1":
            success = True
        else:
            success = False
        csvdr.writerow({
            'date': datetime.datetime.now().isoformat(),
            'success': success,
            'contracts': n,
            'month': month
        })


    month = nextmonth(month)
    # raise(Exception)


a = repo.git.add(path + "data2.csv")

with repo.git.custom_environment(GIT_COMMITTER_NAME=settings.bot_name, GIT_COMMITTER_EMAIL=settings.bot_email):
    try:
        repo.git.commit(message="happily updating data: %s contracts" % str(n), author="%s <%s>" % (settings.bot_name, settings.bot_email))
    except:
        nothing = None
try:
    with repo.git.custom_environment(GIT_SSH_COMMAND=git_ssh_cmd):
        repo.git.push()
except:
        nothing = None
message="happily updating data: %s contracts" % str(n)
print(message)
