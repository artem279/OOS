from lxml import etree, objectify
import os, csv, threading, time, datetime, pyodbc
from zipfile import ZipFile
from shutil import copy2
from threading import Thread
from queue import Queue

csv.register_dialect('csvCommaDialect', delimiter='|', lineterminator='\n')
bad = 'D:\\projects\\common\\oos\\zakupki_223fz_information\\badzip\\'


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
            if nm not in ['daily', 'full', 'Full', 'contract', 'manual', 'dishonestSupplier'] and nm.count('purchase') == 0:
                zipfiles += getfiles(os.path.join(path, nm, 'purchaseContract'))
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
    positionsList = []

    try:
        with ZipFile(kwargs['file']) as z:
            for zf in z.namelist():
                if str(zf).startswith('purchaseContract_') and z.getinfo(zf).file_size > 300:
                    try:
                        doc = etree.fromstring(z.read(zf))
                    except:
                        print('Error!', zf)
                        pass

                    root = cleannamespaces(doc)
                    items = root.find('body')
                    items = items.findall('item')

                    for child in items:
                        contractData = TryExcept(child.find('purchaseContractData'), '')
                        # Основная информация
                        guid = try_catch(child.find('guid'))
                        registrationNumber = try_catch(contractData.find('registrationNumber'))
                        contractRegNumber = try_catch(contractData.find('contractRegNumber'))
                        createDateTime = try_catch(contractData.find('createDateTime'))
                        contractDate = try_catch(contractData.find('contractCreateDate'))
                        publicationDate = try_catch(contractData.find('publicationDateTime'))
                        # fulfillmentDate = try_catch(contractData.find('fulfillmentDate')) --в основном неструктурированный мусор
                        startExecutionDate = try_catch(contractData.find('startExecutionDate'))
                        endExecutionDate = try_catch(contractData.find('endExecutionDate'))
                        lot = TryExcept(contractData.find('lot'), '')
                        subject = try_catch(lot.find('subject'))
                        price = try_catch(contractData.find('price'))
                        if price == '':
                            price = try_catch(contractData.find('sum'))

                        currency = TryExcept(contractData.find('currency'), '')
                        currency = try_catch(currency.find('code'))
                        purchaseNoticeInfo = TryExcept(contractData.find('purchaseInfo'), '')
                        purchaseNoticeNumber = try_catch(purchaseNoticeInfo.find('purchaseNoticeNumber'))

                        version = try_catch(contractData.find('version'))
                        status = try_catch(contractData.find('status'))

                        # Данные заказчика
                        c_inn = try_catch(contractData.find('customerInfo').find('inn'))
                        c_kpp = try_catch(contractData.find('customerInfo').find('kpp'))
                        c_ogrn = try_catch(contractData.find('customerInfo').find('ogrn'))

                        c_legalAddress = try_catch(contractData.find('customerInfo').find('legalAddress'))
                        c_postalAddress = try_catch(contractData.find('customerInfo').find('postalAddress'))

                        c_phone = try_catch(contractData.find('customerInfo').find('phone'))
                        c_fax = try_catch(contractData.find('customerInfo').find('fax'))
                        c_email = try_catch(contractData.find('customerInfo').find('email'))

                        # Данные разместившего заказ
                        p_inn = try_catch(contractData.find('placer').find('mainInfo').find('inn'))
                        p_kpp = try_catch(contractData.find('placer').find('mainInfo').find('kpp'))
                        p_ogrn = try_catch(contractData.find('placer').find('mainInfo').find('ogrn'))

                        p_legalAddress = try_catch(contractData.find('placer').find('mainInfo').find('legalAddress'))
                        p_postalAddress = try_catch(contractData.find('placer').find('mainInfo').find('postalAddress'))

                        p_phone = try_catch(contractData.find('placer').find('mainInfo').find('phone'))
                        p_fax = try_catch(contractData.find('placer').find('mainInfo').find('fax'))
                        p_email = try_catch(contractData.find('placer').find('mainInfo').find('email'))

                        # Данные поставщика
                        supplierInfo = TryExcept(contractData.find('supplier'), '')
                        supplierInfo = TryExcept(supplierInfo.find('mainInfo'), '')
                        s_inn = try_catch(supplierInfo.find('inn'))
                        s_kpp = try_catch(supplierInfo.find('kpp'))
                        s_ogrn = try_catch(supplierInfo.find('ogrn'))

                        address = TryExcept(supplierInfo.find('address'), '')
                        address_region = TryExcept(address.find('region'), '')
                        s_region = try_catch(address_region.find('code'))

                        s_city = try_catch(address.find('oktmoName'))
                        if s_city == '':
                            s_city = try_catch(address.find('area'))

                        s_email = try_catch(address.find('email'))
                        s_phone = try_catch(address.find('phone'))

                        contract = {}
                        contract['guid'] = guid
                        contract['registrationNumber'] = registrationNumber
                        contract['contractRegNumber'] = contractRegNumber
                        contract['purchaseNoticeNumber'] = purchaseNoticeNumber
                        contract['createDateTime'] = createDateTime
                        contract['contractDate'] = contractDate
                        contract['publicationDate'] = publicationDate
                        # contract['fulfillmentDate'] = fulfillmentDate
                        contract['startExecutionDate'] = startExecutionDate
                        contract['endExecutionDate'] = endExecutionDate
                        contract['subject'] = subject.replace('\n', '')
                        contract['price'] = price
                        contract['currency'] = currency
                        contract['version'] = version
                        contract['status'] = status
                        contract['c_inn'] = c_inn
                        contract['c_kpp'] = c_kpp
                        contract['c_ogrn'] = c_ogrn
                        contract['c_legalAddress'] = c_legalAddress.replace('\n', '')
                        contract['c_postalAddress'] = c_postalAddress.replace('\n', '')
                        contract['c_phone'] = c_phone
                        contract['c_fax'] = c_fax
                        contract['c_email'] = c_email
                        contract['p_inn'] = p_inn
                        contract['p_kpp'] = p_kpp
                        contract['p_ogrn'] = p_ogrn
                        contract['p_legalAddress'] = p_legalAddress.replace('\n', '')
                        contract['p_postalAddress'] = p_postalAddress.replace('\n', '')
                        contract['p_phone'] = p_phone
                        contract['p_fax'] = p_fax
                        contract['p_email'] = p_email
                        contract['s_inn'] = s_inn
                        contract['s_kpp'] = s_kpp
                        contract['s_ogrn'] = s_ogrn
                        contract['s_region'] = s_region
                        contract['s_city'] = s_city
                        contract['s_phone'] = s_phone
                        contract['s_email'] = s_email

                        contractList.append(contract)

                        # Данные по лотам
                        contractPositions = contractData.find('contractItems').findall('contractItem')
                        for pos in contractPositions:
                            name = try_catch(pos.find('name'))
                            try:
                                okdp = try_catch(pos.find('okdp').find('code'))
                            except:
                                okdp = ''

                            try:
                                okved = try_catch(pos.find('okved').find('code'))
                            except:
                                okved = ''

                            try:
                                okpd = try_catch(pos.find('okpd').find('code'))
                            except:
                                okpd = ''

                            try:
                                okpd2 = try_catch(pos.find('okpd2').find('code'))
                            except:
                                okpd2 = ''

                            try:
                                okei = try_catch(pos.find('okei').find('code'))
                            except:
                                okei = ''

                            qty = try_catch(pos.find('qty'))

                            position = {}
                            position['guid'] = guid
                            position['name'] = name.replace('\n', '')
                            position['okdp'] = okdp
                            position['okpd'] = okpd
                            position['okpd2'] = okpd2
                            position['okved'] = okved
                            position['okei'] = okei
                            position['qty'] = qty

                            positionsList.append(position)
                    print(zf)
    except Exception as e:
        copy2(kwargs['file'], bad)

    if contractList is not None:
        kwargs['mutex'].acquire()

        with open('contracts_info.csv', 'at', encoding='cp1251', errors='ignore') as file:
            writer = csv.DictWriter(file, ['guid', 'registrationNumber', 'contractRegNumber',
                                           'purchaseNoticeNumber', 'createDateTime', 'contractDate',
                                           'publicationDate', 'startExecutionDate',
                                           'endExecutionDate', 'subject',
                                           'price', 'currency', 'version', 'status', 'c_inn', 'c_kpp', 'c_ogrn',
                                           'c_legalAddress', 'c_postalAddress', 'c_phone', 'c_fax', 'c_email',
                                           'p_inn', 'p_kpp', 'p_ogrn', 'p_legalAddress', 'p_postalAddress',
                                           'p_phone', 'p_fax', 'p_email', 's_inn', 's_kpp', 's_ogrn', 's_region',
                                           's_city', 's_phone', 's_email'], dialect='csvCommaDialect')
            writer.writerows(contractList)

        with open('contracts_info_items.csv', 'at', encoding='cp1251', errors='ignore') as file:
            writer = csv.DictWriter(file, ['guid', 'name', 'okdp', 'okpd', 'okpd2', 'okved', 'okei', 'qty'],
                                    dialect='csvCommaDialect')
            writer.writerows(positionsList)

        kwargs['mutex'].release()


def createthreadparser(thread_count, files):
    mutex = threading.Lock()
    pool = ThreadPool(int(thread_count))
    while len(files) != 0:
        file = files.pop()
        pool.add_task(parsezipxml, file=file, mutex=mutex)
    pool.wait_completion()

    return None


print(threading.active_count())
t0 = time.time()
if os.path.isfile('contracts_info.csv'):
    os.remove('contracts_info.csv')
if os.path.isfile('contracts_info_items.csv'):
    os.remove('contracts_info_items.csv')
root_path = 'D:\\zakupki\\contracts\\ftp.zakupki.gov.ru\\out\\published'
# xml_files = getfiles('D:\\projects\\common\\oos\\zakupki_223fz_information\\badzip')
xml_files = [file for file in getzipfiles(root_path) if (datetime.date.today() - modification_date(file).date()).days <= 32]

createthreadparser(50, xml_files)
# Настраиваем конфиг парсера
FZ223_config = {'source_path': 'D:\\projects\\common\\oos\\zakupki_223fz_information\\', 'file1': 'contracts_info.csv', 'file2': 'contracts_info_items.csv',
             'load_path': '\\\\vm-grabber-dev\\buffer\\agaev\\robots\\',
             'db': 'projects_oos', 'proc': 'dbo.p_fz223_load_catch'}

# Создаём коннект к серверу
conn = pyodbc.connect(r'DRIVER={SQL Server};SERVER=vm-dbs-um\dbsum;DATABASE=projects_oos;Trusted_Connection=True;')
# Создаём объект-курсор
curs = conn.cursor()
# Копируем необходимые файлы для заливки через процедуру
copy2(os.path.join(FZ223_config['source_path'], FZ223_config['file1']), FZ223_config['load_path'])
copy2(os.path.join(FZ223_config['source_path'], FZ223_config['file2']), FZ223_config['load_path'])
# Запускаем процедуру заливки
curs.execute("exec " + FZ223_config['proc'] + " '" + FZ223_config['load_path'] + "'")
# Подтверждаем изменения и закрываем курсор и соединение
curs.commit()
curs.close()
conn.close()

t = time.time() - t0
print('Понадобилось времени (сек.): %fs' % t)
with open('timelog.log', 'at', encoding='cp1251', errors='ignore') as file:
    file.write('Понадобилось времени (сек.): %fs' % t + '\n')
    file.flush()
