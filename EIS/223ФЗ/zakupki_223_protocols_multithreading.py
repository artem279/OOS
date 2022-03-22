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
                    e.count('purchaseProtocol') > 0]
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

        protocollist = []
        lotlist = []
        participantlist = []

        try:
            with ZipFile(self.__zip_file) as z:
                for zf in z.namelist():
                    if str(zf).startswith('purchaseProtocol'):
                        try:
                            doc = etree.fromstring(z.read(zf))
                        except:
                            print('Error!', zf)
                            pass

                        pType = {
                            'purchaseProtocolRZ1AE': 'Протокол рассмотрения первых частей заявок для аукциона в электронной форме',
                            'purchaseProtocolRZOK': 'Протокол рассмотрения заявок для открытого конкурса',
                            'purchaseProtocolRZOA': 'Протокол рассмотрения заявок для открытого аукциона',
                            'purchaseProtocolRZAE': 'Протокол рассмотрения заявок для аукциона в электронной форме',
                            'purchaseProtocolRZ2AE': 'Протокол рассмотрения вторых частей заявок для аукциона в электронной форме',
                            'purchaseProtocolPAEP': 'Протокол проведения закупки у единственного поставщика',
                            'purchaseProtocolPAAE94FZ': 'Протокол проведения аукциона для открытого аукциона в электронной форме (по 94ФЗ)',
                            'purchaseProtocolPAAE': 'Протокол проведения аукциона для открытого аукциона в электронной форме',
                            'purchaseProtocolPAOA': 'Протокол проведения аукциона для открытого аукциона',
                            'purchaseProtocolOSZ': 'Протокол оценки и сопоставления заявок',
                            'purchaseProtocolZK': 'Протокол запроса котировок',
                            'purchaseProtocol': 'Протокол закупки',
                            'purchaseProtocolVK': 'Протокол вскрытия конвертов',
                            'protocolCancellation': 'Сведения об отмене протокола'
                        }

                        root = cleannamespaces(doc)
                        protocol_type = ''
                        for p in pType.keys():
                            try:
                                protocol_type = root.find('body').find('item').find(p + 'Data').tag
                                tip = pType[p]
                                break
                            except:
                                pass

                        # Основная информация
                        protocol = {}
                        protocol['protocol_type_name'] = tip
                        body = TryExcept(root.find('body'), '')
                        item = TryExcept(body.find('item'), '')
                        purchase = TryExcept(item.find(protocol_type), '')
                        protocol['guid'] = try_catch(item.find('guid'))
                        protocol['protocolDate'] = try_catch(purchase.find('createDateTime'))
                        purchaseInfo = TryExcept(purchase.find('purchaseInfo'), '')
                        protocol['purchaseObjectInfo'] = try_catch(purchaseInfo.find('name'))
                        protocol['purchaseName'] = try_catch(purchaseInfo.find('purchaseCodeName'))
                        protocol['purchaseNumber'] = try_catch(purchaseInfo.find('purchaseNoticeNumber'))
                        protocol['protocolType'] = try_catch(purchase.find('typeName'))
                        try:
                            customers = purchase.find('customer').find('mainInfo')
                        except:
                            customers = ''
                        try:
                            placers = purchase.find('placer').find('mainInfo')
                        except:
                            placers = ''
                        # Customers
                        protocol['cInn'] = try_catch(customers.find('inn'))
                        protocol['cKpp'] = try_catch(customers.find('kpp'))
                        protocol['cOgrn'] = try_catch(customers.find('ogrn'))
                        protocol['cFName'] = try_catch(customers.find('fullName'))
                        protocol['cSName'] = try_catch(customers.find('shortName'))
                        protocol['cLegalAddress'] = try_catch(customers.find('legalAddress'))
                        protocol['cPostalAddress'] = try_catch(customers.find('postalAddress'))
                        protocol['cPhone'] = try_catch(customers.find('phone'))
                        protocol['cFax'] = try_catch(customers.find('fax'))
                        protocol['cEmail'] = try_catch(customers.find('email'))

                        # Placers
                        protocol['pInn'] = try_catch(placers.find('inn'))
                        protocol['pKpp'] = try_catch(placers.find('kpp'))
                        protocol['pOgrn'] = try_catch(placers.find('ogrn'))
                        protocol['pFName'] = try_catch(placers.find('fullName'))
                        protocol['pSName'] = try_catch(placers.find('shortName'))
                        protocol['pLegalAddress'] = try_catch(placers.find('legalAddress'))
                        protocol['pPostalAddress'] = try_catch(placers.find('postalAddress'))
                        protocol['pPhone'] = try_catch(placers.find('phone'))
                        protocol['pFax'] = try_catch(placers.find('fax'))
                        protocol['pEmail'] = try_catch(placers.find('email'))

                        # Лоты
                        LotApplications = [lot for lot in purchase.iter('protocolLotApplications')]
                        if len(LotApplications) == 0:
                            LotApplications = purchase.iter(protocol_type.replace('purchase', '')
                                                            .replace('Protocol', 'protocol')
                                                            .replace('Data', '') + 'LotApplications')

                        for lot in LotApplications:
                            lotentity = {}
                            lotentity['guid'] = protocol['guid']
                            lotentity['protocolDate'] = protocol['protocolDate']
                            lotentity['protocolType'] = protocol['protocolType']
                            lotentity['protocol_type_name'] = protocol['protocol_type_name']
                            l = TryExcept(lot.find('lot'), '')
                            lotentity['subject'] = try_catch(l.find('subject'))
                            try:
                                lotentity['currency'] = l.find('currency').find('code').text.replace('\n', '') \
                                    .replace('|', '/').replace('"', '')
                            except:
                                lotentity['currency'] = ''
                            lotentity['initialSum'] = try_catch(l.find('initialSum'))
                            applications = [a for a in lot.iter('application')]
                            for a in applications:
                                participant = {}
                                info = TryExcept(a.find('supplierInfo'), a.find('participantInfo'))
                                participant['guid'] = protocol['guid']
                                participant['protocolDate'] = protocol['protocolDate']
                                participant['protocolType'] = protocol['protocolType']
                                participant['protocol_type_name'] = protocol['protocol_type_name']
                                participant['inn'] = try_catch(info.find('inn'))
                                participant['kpp'] = try_catch(info.find('kpp'))
                                participant['ogrn'] = try_catch(info.find('ogrn'))
                                participant['name'] = try_catch(info.find('name'))
                                participant['address'] = try_catch(info.find('address'))
                                participant['lastPrice'] = try_catch(TryExcept(a.find('lastPrice'), a.find('price')))
                                participantlist.append(participant)
                            lotlist.append(lotentity)

                        protocollist.append(protocol)

        except:
            copy2(self.__zip_file, bad)

        return protocollist, lotlist, participantlist

    def run(self):
        protocollist, lotlist, participantlist = self.parsezipxml()
        if protocollist is not None:
            self.__mutex.acquire()
            with open('protocols.csv', 'at', encoding='cp1251', errors='ignore') as file:
                writer = csv.DictWriter(file, ['guid', 'protocolDate', 'purchaseObjectInfo', 'purchaseName',
                                               'purchaseNumber', 'protocolType', 'protocol_type_name',
                                               'cInn', 'cKpp', 'cOgrn', 'cSName', 'cFName', 'cLegalAddress',
                                               'cPostalAddress', 'cPhone', 'cFax', 'cEmail', 'pInn', 'pKpp',
                                               'pOgrn', 'pSName', 'pFName', 'pLegalAddress', 'pPostalAddress',
                                               'pPhone', 'pFax', 'pEmail'],
                                        dialect='csvCommaDialect')
                writer.writerows(protocollist)

            with open('lots.csv', 'at', encoding='cp1251', errors='ignore') as file:
                writer = csv.DictWriter(file, ['guid', 'protocolDate', 'subject', 'currency', 'initialSum',
                                               'protocolType', 'protocol_type_name'],
                                        dialect='csvCommaDialect')
                writer.writerows(lotlist)

            with open('participants.csv', 'at', encoding='cp1251', errors='ignore') as file:
                writer = csv.DictWriter(file, ['guid', 'protocolDate', 'inn', 'kpp', 'ogrn', 'name',
                                               'lastPrice', 'address', 'protocolType', 'protocol_type_name'],
                                        dialect='csvCommaDialect')
                writer.writerows(participantlist)
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
# xmllist = getfiles('C:/notify_guarantee/protocols223/')
root_path = 'D:\\data\\common\\oos\\contracts\\ftp.zakupki.gov.ru\\out\\published'
xmllist = [file for file in getzipfiles(root_path) if (datetime.date.today() - modification_date(file).date()).days <= 95]
print(len(xmllist))
createthreadparserzip(73, xmllist)
t = time.time() - t0
print('Понадобилось времени (сек.): %fs' % t)
with open('timelog.log', 'at', encoding='cp1251', errors='ignore') as file:
    file.write('Понадобилось времени (сек.): %fs' % t + '\n')
    file.flush()
