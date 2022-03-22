from lxml import etree, objectify
import os, csv, threading, time, datetime
from zipfile import ZipFile
from shutil import copy2

csv.register_dialect('csvCommaDialect', delimiter='|', lineterminator='\n')
bad = 'D:\\projects\\common\\oos\\zakupki_44fz\\badzip\\'


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
            if nm not in ['notifications', 'prevMonth', 'currMonth', 'contracts', 'protocols']:
                zipfiles += getfiles(os.path.join(path, nm, 'notifications'))
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
                    if str(zf).startswith('fcsNotification') and z.getinfo(zf).file_size > 300 and not str(
                            zf).startswith('fcsNotificationCancel'):
                        try:
                            doc = etree.fromstring(z.read(zf))
                        except:
                            print('Error!', zf)
                            pass

                        root = cleannamespaces(doc)

                        notifyentity = {}

                        # Основные данные закупки
                        notify = root.getchildren()[0]
                        if notify.tag == 'fcsNotificationLotChange':
                            continue
                        notifyentity['purchaseNumber'] = try_catch(notify.find('purchaseNumber'))
                        notifyentity['docPublishDate'] = try_catch(notify.find('docPublishDate'))
                        notifyentity['purchaseObjectInfo'] = try_catch(notify.find('purchaseObjectInfo'))
                        modification = TryExcept(notify.find('modification'), '')
                        notifyentity['modification'] = try_catch(modification.find('modificationNumber'))
                        placingWay = TryExcept(notify.find('placingWay'), '')
                        notifyentity['placingWay'] = try_catch(placingWay.find('name'))
                        etp = TryExcept(notify.find('ETP'), '')
                        notifyentity['etp'] = try_catch(etp.find('name'))

                        purchaseResponsible = [p for p in root.iter('purchaseResponsible')][0]
                        responsibleOrg = TryExcept(purchaseResponsible.find('responsibleOrg'), '')
                        responsibleInfo = TryExcept(purchaseResponsible.find('responsibleInfo'), '')
                        contactPerson = TryExcept(responsibleInfo.find('contactPerson'), '')

                        # Данные заказчика
                        notifyentity['c_regNum'] = try_catch(responsibleOrg.find('regNum'))
                        notifyentity['c_inn'] = try_catch(responsibleOrg.find('INN'))
                        notifyentity['c_kpp'] = try_catch(responsibleOrg.find('KPP'))
                        notifyentity['lastName'] = try_catch(contactPerson.find('lastName'))
                        notifyentity['firstName'] = try_catch(contactPerson.find('firstName'))
                        notifyentity['middleName'] = try_catch(contactPerson.find('middleName'))
                        # Контактные данные
                        notifyentity['contactPhone'] = try_catch(responsibleInfo.find('contactPhone'))
                        notifyentity['contactFax'] = try_catch(responsibleInfo.find('contactFax'))
                        notifyentity['contactEMail'] = try_catch(responsibleInfo.find('contactEMail'))

                        # Данные по лотам
                        items = notify.iter('purchaseObject')
                        for item in items:
                            lot = {}
                            lot['purchaseNumber'] = notifyentity['purchaseNumber']
                            lot['modification'] = notifyentity['modification']
                            lot['docPublishDate'] = notifyentity['docPublishDate']
                            # Коды ОКПД/ОКЕИ
                            OKPD = TryExcept(item.find('OKPD'), '')
                            lot['OKPD'] = try_catch(OKPD.find('code'))
                            OKPD2 = TryExcept(item.find('OKPD2'), '')
                            lot['OKPD2'] = try_catch(OKPD2.find('code'))
                            OKEI = TryExcept(item.find('OKEI'), '')
                            lot['OKEI'] = try_catch(OKEI.find('code'))

                            # Информация о лоте (описание/сумма/количество)
                            lot['objname'] = try_catch(item.find('name'))
                            lot['price'] = try_catch(item.find('price'))
                            lot['sum'] = try_catch(item.find('sum'))
                            quantity = TryExcept(item.find('quantity'), '')
                            lot['quantity'] = try_catch(quantity.find('value'))
                            lotlist.append(lot)

                        notifylist.append(notifyentity)

        except:
            copy2(self.__zip_file, bad)

        return notifylist, lotlist

    def run(self):
        notifylist, lotlist = self.parsezipxml()
        if notifylist is not None:
            self.__mutex.acquire()
            with open('notifications.csv', 'at', encoding='cp1251', errors='ignore') as file:
                writer = csv.DictWriter(file, ['purchaseNumber', 'purchaseObjectInfo', 'modification',
                                               'docPublishDate', 'c_inn', 'c_kpp', 'c_regNum', 'placingWay',
                                               'etp', 'lastName', 'firstName', 'middleName', 'contactPhone',
                                               'contactFax', 'contactEMail'],
                                        dialect='csvCommaDialect')
                writer.writerows(notifylist)

            with open('notifications_items.csv', 'at', encoding='cp1251', errors='ignore') as file:
                writer = csv.DictWriter(file,
                                        ['purchaseNumber', 'objname', 'modification', 'docPublishDate', 'OKPD',
                                         'OKPD2', 'OKEI', 'price', 'quantity', 'sum'],
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
# xmllist = getfiles('C:/notify_guarantee/notify44/')
root_path = 'D:\\data\\common\\oos\\contracts\\ftp.zakupki.gov.ru\\fcs_regions'
xmllist = [file for file in getzipfiles(root_path) if (datetime.date.today() - modification_date(file).date()).days <= 95]
createthreadparserzip(49, xmllist)
t = time.time() - t0
print('Понадобилось времени (сек.): %fs' % t)
with open('timelog.log', 'at', encoding='cp1251', errors='ignore') as file:
    file.write('Понадобилось времени (сек.): %fs' % t + '\n')
    file.flush()
