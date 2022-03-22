from lxml import etree, objectify
import os, csv, threading, time, datetime
from zipfile import ZipFile
from shutil import copy2

csv.register_dialect('csvCommaDialect', delimiter='|', lineterminator='\n')
bad = 'D:\\projects\\common\\oos\\zakupki_223fz_information\\badzip\\'


def modification_date(filename):
    return datetime.datetime.fromtimestamp(os.path.getmtime(filename))


def getfiles(path):
    filearray = []
    for top, dirs, files in os.walk(path):
        for nm in files:
            filearray.append(os.path.join(top, nm))
    return filearray


def getzipfiles(path):
    zipfiles = []
    for top, dirs, files in os.walk(path):
        for nm in dirs:
            pdir = [e for _, protocol_dir, _ in os.walk(os.path.join(path, nm)) for e in protocol_dir if
                    e.count('purchaseNotice') > 0]
            for dir in pdir:
                zipfiles += getfiles(os.path.join(path, nm, dir))
    return zipfiles


class ParserThread(threading.Thread):
    def __init__(self, zip_file, mut):
        threading.Thread.__init__(self)
        self.__zip_file = zip_file
        self.__mutex = mut

    def parsezipxml(self):

        def Try_catchDecorator(myFunc):
            def wrapper(value):
                try:
                    val = value.text
                    return myFunc(val)
                except:
                    val = ''
                    return myFunc(val)

            return wrapper

        def TryExceptDecorator(myFunc):
            def wrapper(value1, value2):
                try:
                    val1 = value1.tag
                    return myFunc(value1, value2)
                except:
                    try:
                        val2 = value2.tag
                        return myFunc(value1, value2)
                    except:
                        value1 = ''
                        value2 = ''
                        return myFunc(value1, value2)

            return wrapper

        @TryExceptDecorator
        def TryExcept(val1, val2):
            content = ''
            if val1 is not None:
                content = val1
            else:
                content = val2
            return content

        @Try_catchDecorator
        def try_catch(value):
            content = value.replace('\n', '').replace('\r', '').replace('\t', '').replace('|', '/').replace('"', '')
            return content

        def cleannamespaces(root):
            for elem in root.getiterator():
                if not hasattr(elem.tag, 'find'):
                    continue  # (1)
                i = elem.tag.find('}')
                if i >= 0:
                    elem.tag = elem.tag[i + 1:]

            objectify.deannotate(root, cleanup_namespaces=True)

            return root

        notifylist = []
        lotlist = []

        try:
            with ZipFile(self.__zip_file) as z:
                for zf in z.namelist():
                    if str(zf).startswith('purchaseNotice') and z.getinfo(zf).file_size > 300:
                        try:
                            doc = etree.fromstring(z.read(zf))
                        except:
                            print('Error!', zf)
                            pass

                        root = cleannamespaces(doc)
                        notifyentity = {}

                        # Основные данные закупки
                        body = TryExcept(root.find('body'), '')
                        item = TryExcept(body.find('item'), '')
                        try:
                            notify = item.getchildren()[1]
                        except:
                            notify = item.getchildren()[0]
                        electronicPlaceInfo = TryExcept(notify.find('electronicPlaceInfo'), '')
                        electronicPlaceInfo = TryExcept(electronicPlaceInfo.find('name'), notify.find('urlVSRZ'))

                        guid = try_catch(item.find('guid'))
                        if guid == '':
                            guid = try_catch(notify.find('guid'))
                        # if guid == 'a9898519-0000-0004-0002-000000239671':
                        #     print('Bingo!')
                        notifyentity['guid'] = guid
                        notifyentity['createDateTime'] = try_catch(notify.find('createDateTime'))
                        notifyentity['registrationNumber'] = try_catch(notify.find('registrationNumber'))
                        notifyentity['etp'] = try_catch(electronicPlaceInfo)

                        notifyentity['name'] = try_catch(notify.find('name'))
                        notifyentity['purchaseCodeName'] = try_catch(notify.find('purchaseCodeName'))
                        notifyentity['status'] = try_catch(notify.find('status'))
                        notifyentity['version'] = try_catch(notify.find('version'))
                        notifyentity['modificationDate'] = try_catch(notify.find('modificationDate'))

                        try:
                            customers = notify.find('customer').find('mainInfo')
                        except:
                            customers = ''
                        try:
                            placers = notify.find('placer').find('mainInfo')
                        except:
                            placers = ''

                        # Customers
                        notifyentity['c_inn'] = try_catch(customers.find('inn'))
                        notifyentity['c_kpp'] = try_catch(customers.find('kpp'))
                        notifyentity['c_ogrn'] = try_catch(customers.find('ogrn'))
                        notifyentity['c_Phone'] = try_catch(customers.find('phone'))
                        notifyentity['c_Fax'] = try_catch(customers.find('fax'))
                        notifyentity['c_Email'] = try_catch(customers.find('email'))

                        # Placers
                        notifyentity['p_inn'] = try_catch(placers.find('inn'))
                        notifyentity['p_kpp'] = try_catch(placers.find('kpp'))
                        notifyentity['p_ogrn'] = try_catch(placers.find('ogrn'))
                        notifyentity['p_Phone'] = try_catch(placers.find('phone'))
                        notifyentity['p_Fax'] = try_catch(placers.find('fax'))
                        notifyentity['p_Email'] = try_catch(placers.find('email'))

                        # Карточка контактного лица
                        contact = TryExcept(notify.find('contact'), '')
                        contact_organization = TryExcept(contact.find('organization'), '')
                        contact_organization = TryExcept(contact_organization.find('mainInfo'), '')

                        notifyentity['contact_inn'] = try_catch(contact_organization.find('inn'))
                        notifyentity['contact_kpp'] = try_catch(contact_organization.find('kpp'))
                        notifyentity['contact_ogrn'] = try_catch(contact_organization.find('ogrn'))

                        notifyentity['contact_firstname'] = try_catch(contact.find('firstName'))
                        notifyentity['contact_lastname'] = try_catch(contact.find('lastName'))
                        notifyentity['contact_middlename'] = try_catch(contact.find('middleName'))

                        notifyentity['contact_user_phone'] = try_catch(contact.find('phone'))
                        notifyentity['contact_user_fax'] = try_catch(contact.find('fax'))
                        notifyentity['contact_user_email'] = try_catch(contact.find('email'))

                        notifyentity['contact_org_phone'] = try_catch(contact_organization.find('phone'))
                        notifyentity['contact_org_fax'] = try_catch(contact_organization.find('fax'))
                        notifyentity['contact_org_email'] = try_catch(contact_organization.find('email'))

                        # Лоты
                        lots = notify.iter('lot')
                        summ = float(0)
                        for lot in lots:
                            lotentity = {}
                            lotentity['guid'] = notifyentity['guid']
                            lotentity['createDateTime'] = notifyentity['createDateTime']
                            lotentity['modificationDate'] = notifyentity['modificationDate']
                            lotitems = lot.iter('lotItem')
                            lotData = TryExcept(lot.find('lotData'), '')
                            lotentity['subject'] = ''.join([try_catch(sub) for sub in lot.iter('subject')])
                            test = ''
                            try:
                                test = [float(try_catch(s).replace(',', '.')) for s in lot.iter('initialSum')]
                                if len(test) == 0:
                                    test = [float(try_catch(s).replace(',', '.')) for s in lot.iter('initialSumInfo')]
                            except:
                                test = [0]
                            summ += sum(test)
                            lotentity['currency'] = ''.join(
                                [try_catch(cur.find('code')) for cur in lot.iter('currency')])

                            for item in lotitems:
                                okdp = TryExcept(item.find('okdp'), '')
                                lotentity['okdp'] = try_catch(okdp.find('code'))
                                okpd2 = TryExcept(item.find('okpd2'), '')
                                lotentity['okpd2'] = try_catch(okpd2.find('code'))
                                okved = TryExcept(item.find('okved'), '')
                                lotentity['okved'] = try_catch(okved.find('code'))
                                okved2 = TryExcept(item.find('okved2'), '')
                                lotentity['okved2'] = try_catch(okved2.find('code'))
                                okei = TryExcept(item.find('okei'), '')
                                lotentity['okei'] = try_catch(okei.find('code'))
                                lotentity['qty'] = try_catch(item.find('qty'))
                                lotlist.append(lotentity)

                        notifyentity['sum'] = round(summ, 2)
                        notifylist.append(notifyentity)

        except:
            copy2(self.__zip_file, bad)

        return notifylist, lotlist

    def run(self):
        notifylist, lotlist = self.parsezipxml()
        if notifylist is not None:
            self.__mutex.acquire()
            with open('notifications.csv', 'at', encoding='cp1251', errors='ignore') as file:
                writer = csv.DictWriter(file,
                                        ['guid', 'createDateTime', 'modificationDate', 'registrationNumber',
                                         'name', 'purchaseCodeName', 'version', 'status', 'etp', 'sum', 'c_inn',
                                         'c_kpp', 'c_ogrn', 'c_Phone', 'c_Fax', 'c_Email', 'p_inn', 'p_kpp',
                                         'p_ogrn', 'p_Phone', 'p_Fax', 'p_Email', 'contact_inn', 'contact_kpp',
                                         'contact_ogrn', 'contact_lastname', 'contact_firstname',
                                         'contact_middlename', 'contact_user_phone', 'contact_user_fax',
                                         'contact_user_email', 'contact_org_phone', 'contact_org_fax',
                                         'contact_org_email'],
                                        dialect='csvCommaDialect')
                writer.writerows(notifylist)

            with open('notifications_items.csv', 'at', encoding='cp1251', errors='ignore') as file:
                writer = csv.DictWriter(file,
                                        ['guid', 'createDateTime', 'modificationDate', 'subject', 'currency',
                                         'version', 'okdp', 'okpd2', 'okei', 'okved', 'okved2', 'qty'],
                                        dialect='csvCommaDialect')
                writer.writerows(lotlist)
            self.__mutex.release()


def createthreadparserzip(count, zip_files):
    mutex = threading.Lock()
    listthrd = []
    while len(zip_files) != 0:
        print(threading.active_count())
        if threading.active_count() <= count:
            file = zip_files.pop()
            print(file)
            parserthrd = ParserThread(file, mutex)
            parserthrd.start()
            listthrd.append(parserthrd)
        else:
            time.sleep(0.1)
    for thrd in listthrd:
        thrd.join()
    return None


print(threading.active_count())
t0 = time.time()
# xmllist = getfiles('C:/notify_guarantee/notify223/')
root_path = 'D:\\data\\common\\oos\\contracts\\ftp.zakupki.gov.ru\\out\\published'
xmllist = [file for file in getzipfiles(root_path) if (datetime.date.today() - modification_date(file).date()).days <= 95]
print(len(xmllist))
createthreadparserzip(73, xmllist)
t = time.time() - t0
print('Понадобилось времени (сек.): %fs' % t)
with open('timelog.log', 'at', encoding='cp1251', errors='ignore') as file:
    file.write('Понадобилось времени (сек.): %fs' % t + '\n')
    file.flush()
