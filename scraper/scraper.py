# get lists of new contracts (published after last succesful run)

import csv
import datetime
import git
import json
from lxml import etree
import openpyxl
import os
import requests
import xmltodict

import settings

# repo settings
repo = git.Repo(settings.git_dir)
git_ssh_identity_file = settings.ssh_file
o = repo.remotes.origin
git_ssh_cmd = 'ssh -i %s' % git_ssh_identity_file

path = settings.git_dir + "data/"

# get last date
with open(path + "log.csv") as fin:
    csvdr = csv.DictReader(fin)
    for row in csvdr:
        if row['success']:
            lastday = row['last_day']

# get table in XLSX
url = 'https://smlouvy.gov.cz/vyhledavani'
params = {
    'publication_date': {'from': lastday},
    'export': 'Exportovat+do+XLSX',
    'all_versions': '0',
    'do': 'detailedSearchForm-submit'
}
r = requests.get(url, params=params)
ok = True
if r.status_code == 200:
    # save table in XLSX
    with open(path + "data.xlsx", "wb") as fout:
        fout.write(r.content)


    # get existing table in CSV
    table_ids = []  #'https://smlouvy.gov.cz/smlouva/5901'
    existing_ids = []   #'5901'
    with open(path + "table.csv") as fin:
        csvdr = csv.DictReader(fin)
        for row in csvdr:
            table_ids.append(row['Adresa záznamu'])
            existing_ids.append(row['Adresa záznamu'].split('/')[-1])

    # read table from XLSX
    newtabledata = []
    i = 0
    wb = openpyxl.load_workbook(path + "data.xlsx")
    for ws in wb:
        for row in ws:
            if i == 0:
               j = 0
               for cell in row:
                    if cell.value == 'Adresa záznamu':
                        idc = j
                    j += 1
            else:
                if not row[idc].value in table_ids:
                    newline = []
                    for cell in row:
                        newline.append(cell.value)
                    newtabledata.append(newline)
                    existing_ids.append(row[idc].value.split('/')[-1])

            i += 1

    # write new lines
    with open (path + "table.csv", "a") as fout:
        csvw = csv.writer(fout)
        for row in newtabledata:
            csvw.writerow(row)

    # read data:
    data = []
    data_ids = []
    with open(path + "data.csv") as fin:
        reader = csv.reader(fin)
        header = next(reader)
    with open(path + "data.csv") as fin:
        csvdr = csv.DictReader(fin)
        for row in csvdr:
            data.append(row)
            data_ids.append(row['id'])


    def name2name(h,obj):
        try:
            if h == 'contract_id':
                return obj['zaznam']['data']['identifikator']['idSmlouvy']
            elif h == 'version_id' or h == 'id':
                return obj['zaznam']['data']['identifikator']['idVerze']
            elif h == 'url':
                return obj['zaznam']['data']['odkaz']
            elif h == 'published':
                return obj['zaznam']['data']['casZverejneni']
            elif h == 'principal:name':
                return obj['zaznam']['data']['smlouva']['subjekt']['nazev']
            elif h == 'principal:id':
                return obj['zaznam']['data']['smlouva']['subjekt']['ico']
            elif h == 'principal:address':
                return obj['zaznam']['data']['smlouva']['subjekt']['adresa']
            elif h == 'principal:ds':
                return obj['zaznam']['data']['smlouva']['subjekt']['datovaSchranka']
            elif h == 'subject':
                return obj['zaznam']['data']['smlouva']['predmet']
            elif h == 'concluded':
                return obj['zaznam']['data']['smlouva']['datumUzavreni']
            elif h == 'number':
                return obj['zaznam']['data']['smlouva']['cisloSmlouvy']
            elif h == 'approved_by':
                return obj['zaznam']['data']['smlouva']['schvalil']
            elif h == 'value':
                try:
                    return obj['zaznam']['data']['smlouva']['hodnotaVcetneDph']
                except:
                    return obj['zaznam']['data']['smlouva']['ciziMena']['hodnota']
            elif h == 'value_without_vat':
                return obj['zaznam']['data']['smlouva']['hodnotaBezDph']
            elif h == 'currency':
                try:
                    return obj['zaznam']['data']['smlouva']['ciziMena']['mena']
                except:
                    return 'CZK'
            elif h == 'contractors':
                try:
                    obj['zaznam']['data']['smlouva']['smluvniStrana'][0]
                    return json.dumps(obj['zaznam']['data']['smlouva']['smluvniStrana'])
                except:
                    try:
                        return json.dumps([obj['zaznam']['data']['smlouva']['smluvniStrana']])
                    except:
                        return json.dumps([])
        except:
            return ''


    # read all XMLs:
    n = 0
    with open (path + "data.csv", "a") as fout:
        csvdw = csv.DictWriter(fout, fieldnames=header)
        for eid in existing_ids:
            if not eid in data_ids:
                url = "https://smlouvy.gov.cz/smlouva/" + eid + "/xml"
                r = requests.get(url)
                print(str(n) + ": " + url)
                n += 1
                if r.status_code == 200:
                    with open(path + "dev/" + eid + ".xml", "wb") as fxml:
                        fxml.write(r.content)

                    with open(path + "dev/" + eid + ".xml") as fxml:
                        tree = etree.parse(path + "dev/" + eid + ".xml").getroot()
                    os.remove(path + "dev/" + eid + ".xml")
                    try:
                        namespace = tree.nsmap[None]
                        elem = tree.xpath('//x:prilohy', namespaces={'x': namespace})[0]
                        elem.getparent().remove(elem)
                    except:
                        nothing = None
                    xmlstring = etree.tostring(tree)
                    djson = xmltodict.parse(xmlstring)
                    newitem = {}
                    for h in header:
                        newitem[h] = name2name(h,djson)
                    csvdw.writerow(newitem)
                else:
                    ok = False
                # raise(Exception)

with open(path + "log.csv", "a") as fin:
    reader = csv.reader(fin)
    header = next(reader)
with open(path + "log.csv", "a") as fin:
    csvdr = csv.DictWriter(fin, fieldnames=header)
    csvdr.writerow({
        'date': datetime.datetime.now().isoformat(),
        'success': ok,
        'contracts': n,
        'last_day': lastday
    })


# bots text for commit


a = repo.git.add(settings.git_dir + path + "log.csv")
a = repo.git.add(settings.git_dir + path + "data.csv")
a = repo.git.add(settings.git_dir + path + "table.csv")

with repo.git.custom_environment(GIT_COMMITTER_NAME=settings.bot_name, GIT_COMMITTER_EMAIL=settings.bot_email):
    repo.git.commit(message="happily updating data: %s contracts" % str(n), author="%s <%s>" % (settings.bot_name, settings.bot_email))
with repo.git.custom_environment(GIT_SSH_COMMAND=git_ssh_cmd):
        o.push()
message="happily updating data: %s contracts" % str(n)
