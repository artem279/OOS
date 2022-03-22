import time
from threading import Thread
import threading
from queue import Queue
import csv, os
from lxml import etree, objectify
from zipfile import ZipFile
from shutil import copy2

csv.register_dialect('csvCommaDialect', delimiter='|', lineterminator='\n')
bad = 'C:\\Users\\artem279\\PycharmProjects\\zakupki_44fz\\badzip\\'


class Task(Thread):
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kwargs = self.tasks.get()
            try:
                func(*args, **kwargs)
            except Exception as e:
                print(e)
            finally:
                self.tasks.task_done()


class ThreadPool:
    """Pool of threads consuming tasks from a queue"""

    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Task(self.tasks)

    def add_task(self, func, *args, **kwargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kwargs))

    def wait_completion(self):
        """block until all tasks are done"""
        self.tasks.join()


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
                zipfiles += getfiles(os.path.join(path, nm, 'contracts'))
    return zipfiles


def parsezipxml(**kwargs):
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

    contractList = []
    productList = []
    supplierList = []

    try:
        with ZipFile(kwargs['file']) as z:
            for zf in z.namelist():
                if str(zf).startswith('contract_') and z.getinfo(zf).file_size > 300:
                    try:
                        doc = etree.fromstring(z.read(zf))
                    except:
                        print('Error!', zf)
                        pass

                    root = cleannamespaces(doc)
                    items = root.findall('contract')
                    # contractList = []

                    for contract in items:
                        contractInfo = {}

                        # Базовая информация
                        purchaseNoticeNumber = ''
                        try:
                            purchaseNoticeNumber = contract.find('foundation').find('fcsOrder') \
                                .find('notificationNumber').text
                        except:
                            try:
                                purchaseNoticeNumber = contract.find('foundation').find('fcsOrder').find('order') \
                                    .find('notificationNumber').text
                            except:
                                try:
                                    purchaseNoticeNumber = contract.find('foundation').find('oosOrder').find(
                                        'order') \
                                        .find('notificationNumber').text
                                except:
                                    purchaseNoticeNumber = ''

                        regNum = try_catch(contract.find('regNum'))
                        signDate = try_catch(contract.find('signDate'))
                        publishDate = try_catch(contract.find('publishDate'))
                        versionNumber = try_catch(contract.find('versionNumber'))
                        priceInfo = TryExcept(contract.find('priceInfo'), '')
                        currency_code = TryExcept(contract.find('currency'), priceInfo.find('currency'))
                        currency_code = try_catch(currency_code.find('code'))
                        price = try_catch(contract.find('price'))
                        if price == '':
                            price = try_catch(priceInfo.find('price'))
                            if price == '':
                                price = try_catch(priceInfo.find('priceRUR'))

                        executionDate = TryExcept(contract.find('executionDate'), '')
                        executionDate_month = try_catch(executionDate.find('month'))
                        executionDate_year = try_catch(executionDate.find('year'))
                        executionPeriod = TryExcept(contract.find('executionPeriod'), '')
                        execution_startDate = try_catch(executionPeriod.find('startDate'))
                        execution_endDate = try_catch(executionPeriod.find('endDate'))
                        modification = TryExcept(contract.find('modification'), '')
                        modification = try_catch(modification.find('type'))

                        # Информация о заказчике и разместившем заказ
                        customer = TryExcept(contract.find('customer'), '')
                        c_regNum = try_catch(customer.find('regNum'))
                        c_inn = try_catch(customer.find('inn'))
                        c_kpp = try_catch(customer.find('kpp'))
                        placer = TryExcept(contract.find('placer'), '')
                        placer = TryExcept(placer.find('responsibleOrg'), '')
                        placer_regNum = try_catch(placer.find('regNum'))

                        contractInfo['regNum'] = regNum
                        contractInfo['purchaseNoticeNumber'] = purchaseNoticeNumber
                        contractInfo['signDate'] = signDate
                        contractInfo['publishDate'] = publishDate
                        contractInfo['versionNumber'] = versionNumber
                        contractInfo['currency_code'] = currency_code
                        contractInfo['price'] = price
                        contractInfo['executionDate_month'] = executionDate_month
                        contractInfo['executionDate_year'] = executionDate_year
                        contractInfo['execution_startDate'] = execution_startDate
                        contractInfo['execution_endDate'] = execution_endDate
                        contractInfo['modification'] = modification
                        contractInfo['c_regNum'] = c_regNum
                        contractInfo['c_inn'] = c_inn
                        contractInfo['c_kpp'] = c_kpp
                        contractInfo['placer_regNum'] = placer_regNum

                        # Продукты
                        products = TryExcept(contract.find('products'), '')
                        products = products.findall('product')
                        # productList = []
                        for p in products:
                            productInfo = {}
                            p_name = try_catch(p.find('name'))
                            p_OKPD = TryExcept(p.find('OKPD'), '')
                            p_OKPD = try_catch(p_OKPD.find('code'))
                            p_OKPD2 = TryExcept(p.find('OKPD2'), '')
                            p_OKPD2 = try_catch(p_OKPD2.find('code'))
                            p_OKEI = TryExcept(p.find('OKEI'), '')
                            p_OKEI = try_catch(p_OKEI.find('code'))
                            p_price = TryExcept(p.find('price'), p.find('priceRUR'))
                            p_price = try_catch(p_price)
                            p_quantity = try_catch(p.find('quantity'))
                            p_sum = TryExcept(p.find('sum'), p.find('sumRUR'))
                            p_sum = try_catch(p_sum)

                            productInfo['regNum'] = regNum
                            productInfo['p_name'] = p_name
                            productInfo['p_OKPD'] = p_OKPD
                            productInfo['p_OKPD2'] = p_OKPD2
                            productInfo['p_OKEI'] = p_OKEI
                            productInfo['p_price'] = p_price
                            productInfo['p_sum'] = p_sum
                            productInfo['p_versionNumber'] = versionNumber
                            productInfo['p_quantity'] = p_quantity

                            productList.append(productInfo)

                        # Поставщики
                        suppliers = TryExcept(contract.find('suppliers'), '')
                        try:
                            suppliers = suppliers.findall('supplier')
                        except:
                            suppliers = []
                        # supplierList = []
                        for s in suppliers:
                            supplierInfo = {}
                            legalEntity = TryExcept(s.find('legalEntityRF'), s.find('individualPersonRF'))
                            s_inn = TryExcept(s.find('inn'), s.find('INN'))
                            try:
                                s_inn = s_inn.text.replace('\n', '').replace('\r', '').replace('\t', '') \
                                    .replace('|', '/').replace('"', '')
                            except:
                                s_inn = try_catch(legalEntity.find('INN'))

                            s_kpp = TryExcept(s.find('kpp'), s.find('KPP'))
                            try:
                                s_kpp = s_kpp.text.replace('\n', '').replace('\r', '').replace('\t', '') \
                                    .replace('|', '/').replace('"', '')
                            except:
                                s_kpp = try_catch(legalEntity.find('KPP'))

                            s_countryCode = TryExcept(s.find('country'), '')
                            s_countryCode = try_catch(s_countryCode.find('countryCode'))

                            # Контакты
                            s_contactPhone = try_catch(s.find('contactPhone'))
                            if s_contactPhone == '':
                                s_contactPhone = try_catch(legalEntity.find('contactPhone'))

                            s_contactEMail = try_catch(s.find('contactEMail'))
                            if s_contactEMail == '':
                                s_contactEMail = try_catch(legalEntity.find('contactEMail'))

                            # ФИО
                            contactInfo = TryExcept(legalEntity.find('contactInfo'), s.find('individualPersonRF'))
                            try:
                                lastName = s.find('contactInfo').find('lastName').text.replace('\n', '') \
                                    .replace('\r', '').replace('\t', '').replace('|', '/').replace('"', '')
                            except:
                                lastName = try_catch(contactInfo.find('lastName'))

                            try:
                                firstName = s.find('contactInfo').find('firstName').text.replace('\n', '') \
                                    .replace('\r', '').replace('\t', '').replace('|', '/').replace('"', '')
                            except:
                                firstName = try_catch(contactInfo.find('firstName'))

                            try:
                                middleName = s.find('contactInfo').find('middleName').text.replace('\n', '') \
                                    .replace('\r', '').replace('\t', '').replace('|', '/').replace('"', '')
                            except:
                                middleName = try_catch(contactInfo.find('middleName'))

                            fio = lastName + ' ' + firstName + ' ' + middleName

                            supplierInfo['regNum'] = regNum
                            supplierInfo['s_inn'] = s_inn
                            supplierInfo['s_kpp'] = s_kpp
                            supplierInfo['s_countryCode'] = s_countryCode
                            supplierInfo['s_contactPhone'] = s_contactPhone
                            supplierInfo['s_contactEMail'] = s_contactEMail
                            supplierInfo['s_fio'] = fio
                            supplierInfo['s_versionNumber'] = versionNumber

                            supplierList.append(supplierInfo)

                        contractList.append(contractInfo)

                    print(zf)
    except:
        copy2(kwargs['file'], bad)

    if contractList is not None:
        kwargs['mutex'].acquire()
        with open('contracts.csv', 'at', encoding='cp1251', errors='ignore') as file:
            writer = csv.DictWriter(file, ['regNum', 'purchaseNoticeNumber', 'versionNumber', 'modification',
                                           'signDate', 'publishDate', 'price', 'currency_code',
                                           'executionDate_month', 'executionDate_year', 'execution_startDate',
                                           'execution_endDate', 'c_regNum', 'placer_regNum', 'c_inn', 'c_kpp'],
                                    dialect='csvCommaDialect')
            writer.writerows(contractList)

        with open('products.csv', 'at', encoding='cp1251', errors='ignore') as file:
            writer = csv.DictWriter(file, ['regNum', 'p_versionNumber', 'p_name', 'p_OKPD', 'p_OKPD2',
                                           'p_OKEI', 'p_price', 'p_quantity', 'p_sum'],
                                    dialect='csvCommaDialect')
            writer.writerows(productList)

        with open('suppliers.csv', 'at', encoding='cp1251', errors='ignore') as file:
            writer = csv.DictWriter(file, ['regNum', 's_versionNumber', 's_inn', 's_kpp', 's_countryCode',
                                           's_contactPhone', 's_contactEMail', 's_fio'],
                                    dialect='csvCommaDialect')
            writer.writerows(supplierList)
        kwargs['mutex'].release()


def createthreadparser(thread_count, files):
    mutex = threading.Lock()
    pool = ThreadPool(int(thread_count))
    while len(files) != 0:
        file = files.pop()
        pool.add_task(parsezipxml, file=file, mutex=mutex)
    pool.wait_completion()

    return None


t0 = time.time()
root_path = 'D:\\zakupki\\contracts\\ftp.zakupki.gov.ru\\fcs_regions'
xmllist = getzipfiles(root_path)
createthreadparser(50, xmllist)
t = time.time() - t0
print('Понадобилось времени (сек.): %fs' % t)
with open('timelog.log', 'at', encoding='cp1251', errors='ignore') as file:
    file.write('Понадобилось времени (сек.): %fs' % t + '\n')
    file.flush()
