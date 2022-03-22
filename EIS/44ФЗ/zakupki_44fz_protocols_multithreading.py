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
                zipfiles += getfiles(os.path.join(path, nm, 'protocols'))
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

        protocolList = []
        commissionMembersList = []
        participantList = []

        try:
            with ZipFile(self.__zip_file) as z:
                for zf in z.namelist():
                    if str(zf).startswith('fcsProtocol') and z.getinfo(zf).file_size > 300:
                        try:
                            doc = etree.fromstring(z.read(zf))
                        except:
                            print('Error!', zf)
                            pass

                        pType = {
                            "fcsProtocolEF1": "Протокол рассмотрения заявок на участие в электронном аукционе",
                            "fcsProtocolEF2": "Протокол проведения электронного аукциона",
                            "fcsProtocolEF3": "Протокол подведения итогов электронного аукциона",
                            "fcsProtocolEFInvalidation": "Протокол о признании электронного аукциона несостоявшимся",
                            "fcsProtocolEFSingleApp": "Протокол рассмотрения единственной заявки на участие в электронном аукционе",
                            "fcsProtocolEFSinglePart": "Протокол рассмотрения заявки единственного участника электронного аукциона",
                            "fcsProtocolOK1": "Протокол вскрытия конвертов и открытия доступа к электронным документам заявок участников в ОК открытом конкурсе",
                            "fcsProtocolOK2": "Протокол рассмотрения и оценки заявок на участие в ОК открытом конкурсе",
                            "fcsProtocolOKSingleApp": "Протокол рассмотрения единственной заявки на участие в ОК открытом конкурсе",
                            "fcsProtocolOKOU1": "Протокол вскрытия конвертов и открытия доступа к электронным документам заявок участников в ОК-ОУ открытом конкурсе с ограниченным участием",
                            "fcsProtocolOKOU2": "Протокол предквалификационного отбора в ОК-ОУ открытом конкурсе с ограниченным участием",
                            "fcsProtocolOKOU3": "Протокол рассмотрения и оценки заявок на участие в конкурсе в ОК-ОУ открытом конкурсе с ограниченным участием",
                            "fcsProtocolOKOUSingleApp": "Протокол рассмотрения единственной заявки в ОК-ОУ открытом конкурсе с ограниченным участием",
                            "fcsProtocolOKD1": "Протокол вскрытия конвертов и открытия доступа к электронным документам первоначальных заявок участников в ОК-Д двухэтапном конкурсе",
                            "fcsProtocolOKD2": "Протокол предквалификационного отбора в ОК-Д двухэтапном конкурсе",
                            "fcsProtocolOKD3": "Протокол первого этапа в ОК-Д двухэтапном конкурсе",
                            "fcsProtocolOKD4": "Протокол вскрытия конвертов и открытия доступа к электронным документам окончательных заявок участников в ОК-Д двухэтапном конкурсе",
                            "fcsProtocolOKD5": "Протокол рассмотрения и оценки заявок на участие в конкурсе в ОК-Д двухэтапном конкурсе",
                            "fcsProtocolOKDSingleApp": "Протокол рассмотрения единственной заявки в ОК-Д двухэтапном конкурсе",
                            "fcsProtocolZK": "Протокол рассмотрения и оценки заявок в ЗК запросе котировок",
                            "fcsProtocolZKAfterProlong": "Протокол рассмотрения и оценки заявок по результатам продления срока подачи заявок в ЗК запросе котировок",
                            "fcsProtocolZKBIAfterProlong": "Протокол рассмотрения и оценки заявок на участие в ЗК-БИ запросе котировок без размещения извещения по результатам продления срока подачи заявок",
                            "fcsProtocolZKBI": "Общая информация об объекте закупки и структурированный протокол рассмотрения и оценки заявок на участие в ЗК-БИ запрос котировок без извещения",
                            "fcsProtocolPO": "Протокол предварительного отбора в ПО предварительном отборе",
                            "fcsProtocolZP": "Протокол проведения запроса предложений в ЗП запросе предложений",
                            "fcsProtocolZPFinal": "Итоговый протокол в ЗП запросе предложений",
                            "fcsProtocolZPExtract": "Протокол выписки из протокола проведения запроса предложений в ЗП запросе предложений",
                            "fcsProtocolEvasion": "Протокол об отказе от заключения контракта",
                            "fcsProtocolCancel": "Информация об отмене протокола",
                            "fcsProtocolDeviation": "Протокол признания участника уклонившимся от заключения контракта"
                        }
                        root = cleannamespaces(doc)
                        protocol_type = ''
                        for p in pType.keys():
                            try:
                                protocol_type = root.find(p).tag
                                break
                            except:
                                pass

                        protocol = {}
                        protocol['protocol_type_name'] = pType[protocol_type]
                        pInfo = TryExcept(root.find(protocol_type), '')
                        protocol['purchaseNumber'] = try_catch(pInfo.find('purchaseNumber'))
                        protocol['protocolDate'] = try_catch(pInfo.find('protocolDate'))
                        protocol['signDate'] = try_catch(pInfo.find('signDate'))
                        protocol['publishDate'] = try_catch(pInfo.find('publishDate'))
                        protocol['docPublishDate'] = try_catch(pInfo.find('docPublishDate'))
                        PublisherSPZ = ''

                        try:
                            PublisherSPZ = try_catch(
                                pInfo.find('protocolPublisher').find('publisherOrg').find('regNum'))
                        except:
                            try:
                                PublisherSPZ = try_catch(
                                    pInfo.find('purchaseInfo').find('purchaseResponsible').find('responsibleOrg').
                                        find('regNum'))
                            except:
                                try:
                                    PublisherSPZ = try_catch(
                                        pInfo.find('purchaseResponsible').find('responsibleOrg').find('regNum'))
                                except:
                                    PublisherSPZ = ''

                        protocol['PublisherSPZ'] = PublisherSPZ

                        try:
                            PublisherName = try_catch(
                                pInfo.find('protocolPublisher').find('publisherOrg').find('fullName'))
                        except:
                            try:
                                PublisherName = try_catch(
                                    pInfo.find('purchaseInfo').find('purchaseResponsible').find('responsibleOrg').
                                        find('fullName'))
                            except:
                                try:
                                    PublisherName = try_catch(
                                        pInfo.find('purchaseResponsible').find('responsibleOrg').find('fullName'))
                                except:
                                    PublisherName = ''

                        protocol['PublisherName'] = PublisherName
                        protocol['purchaseObjectInfo'] = try_catch(pInfo.find('purchaseObjectInfo'))
                        commissionMembers = []
                        try:
                            commissionMembers = pInfo.find('commission').find('commissionMembers').findall(
                                'commissionMember')
                        except:
                            try:
                                commissionMembers = pInfo.find('protocolLot').find('commission').find(
                                    'commissionMembers') \
                                    .findall('commissionMember')
                            except:
                                try:
                                    commissionMembers = pInfo.find('protocolLot').find('comission').find(
                                        'commissionMembers').findall('commissionMember')
                                except:
                                    commissionMembers = []
                        # Данные по комиссии

                        for cmember in commissionMembers:
                            commissionMember = {}
                            commissionMember['protocol_type_name'] = protocol['protocol_type_name']
                            commissionMember['purchaseNumber'] = protocol['purchaseNumber']
                            commissionMember['protocolDate'] = protocol['protocolDate']
                            commissionMember['lastName'] = try_catch(cmember.find('lastName'))
                            commissionMember['firstName'] = try_catch(cmember.find('firstName'))
                            commissionMember['middleName'] = try_catch(cmember.find('middleName'))
                            commissionMember['role'] = try_catch(cmember.find('role').find('name'))
                            commissionMembersList.append(commissionMember)

                        # Данные по участникам

                        try:
                            protocolLots = pInfo.find('protocolLots').findall('protocolLot')
                        except:
                            try:
                                protocolLots = pInfo.findall('protocolLot')
                            except:
                                print('Нет данных по лотам и участникам!')
                                # with open('log.txt', 'at', encoding='utf-8') as file:
                                #     file.write(f + '|Нет данных по лотам и участникам!|' + zf + '\n')
                                #     file.flush()

                        for p in protocolLots:
                            try:
                                participants = p.find('application').find('appParticipants').findall('appParticipant')
                            except:
                                try:
                                    participants = p.find('applications').findall('application')
                                except:
                                    try:
                                        participants = p.findall('application')
                                    except:
                                        print('Нет данных по участникам!')
                                        # with open('log.txt', 'at', encoding='utf-8') as file:
                                        #     file.write(f + '|Нет данных по участникам!|' + zf + '\n')
                                        #     file.flush()

                            # if len(participants) == 0:
                            #     print('Bingo!')

                            for a in participants:
                                participant = {}
                                participant['protocol_type_name'] = protocol['protocol_type_name']
                                participant['purchaseNumber'] = protocol['purchaseNumber']
                                participant['protocolDate'] = protocol['protocolDate']
                                appData = TryExcept(p.find('application'), '')
                                participant['appDate'] = try_catch(appData.find('appDate'))
                                application = TryExcept(p.find('application'), '')
                                offerPrice = TryExcept(
                                    TryExcept(application.find('price'), application.find('offerPrice')),
                                    application.find('offer'))
                                participant['offerPrice'] = try_catch(offerPrice)
                                # if participant['offerPrice'] != '':
                                #     print('Bingo!')
                                try:
                                    participant['protocol_type_name'] = protocol['protocol_type_name']
                                    participant['purchaseNumber'] = protocol['purchaseNumber']
                                    participant['protocolDate'] = protocol['protocolDate']
                                    participant['inn'] = a.find('inn').text.strip().replace('\n', '').replace('|', '/') \
                                        .replace('"', '')
                                    participant['kpp'] = a.find('kpp').text.strip().replace('\n', '').replace('|', '/') \
                                        .replace('"', '')
                                    participant['organizationName'] = a.find('organizationName').text.strip(). \
                                        replace('\n', '').replace('|', '/').replace('"', '')
                                    participant['postAddress'] = a.find('postAddress').text.strip().replace('\n', '') \
                                        .replace('|', '/').replace('"', '')
                                except:
                                    try:
                                        offerPrice = TryExcept(a.find('contractConditions'), '')
                                        offerPrice = [row for row in offerPrice.findall('contractCondition')
                                                      if try_catch(row.find('offer')) != ''][0]
                                        offerPrice = TryExcept(offerPrice, TryExcept(a.find('appParticipant'),
                                                                                     a.find('priceOffers')))
                                        if offerPrice is not None and offerPrice != '':
                                            if offerPrice.tag == 'priceOffers':
                                                offerPrice = TryExcept(offerPrice.find('firstOffer'),
                                                                       offerPrice.find('lastOffer'))
                                                offerPrice = try_catch(offerPrice.find('price'))
                                            else:
                                                offerPrice = TryExcept(offerPrice.find('offerPrice'),
                                                                       TryExcept(offerPrice.find('offer'),
                                                                                 offerPrice.find('price')))
                                    except:
                                        offerPrice = ''
                                    participant['offerPrice'] = try_catch(offerPrice)
                                    appDate = TryExcept(a.find('priceOffers'), '')
                                    appDate = TryExcept(appDate.find('firstOffer'), appDate.find('lastOffer'))
                                    appDate = TryExcept(a.find('appDate'), appDate.find('date'))
                                    participant['appDate'] = try_catch(appDate)
                                    participant['protocol_type_name'] = protocol['protocol_type_name']
                                    participant['purchaseNumber'] = protocol['purchaseNumber']
                                    participant['protocolDate'] = protocol['protocolDate']
                                    try:
                                        participant['inn'] = a.find('appParticipant').find('inn').text.strip() \
                                            .replace('\n', '').replace('|', '/').replace('"', '')
                                        participant['kpp'] = a.find('appParticipant').find('kpp').text.strip() \
                                            .replace('\n', '').replace('|', '/').replace('"', '')
                                        participant['organizationName'] = a.find('appParticipant') \
                                            .find('organizationName').text.strip().replace('\n', '').replace('|', '/') \
                                            .replace('"', '')
                                        participant['postAddress'] = a.find('appParticipant').find('postAddress').text \
                                            .strip().replace('\n', '').replace('|', '/').replace('"', '')
                                    except:
                                        try:
                                            appParticipants = a.find('appParticipants').findall('appParticipant')
                                            for ap in appParticipants:
                                                participant['inn'] = ap.find('inn').text.strip().replace('\n', '') \
                                                    .replace('|', '/').replace('"', '')
                                                participant['kpp'] = ap.find('kpp').text.strip().replace('\n', '') \
                                                    .replace('|', '/').replace('"', '')
                                                participant['organizationName'] = ap.find('organizationName').text \
                                                    .strip().replace('\n', '').replace('|', '/').replace('"', '')
                                                participant['postAddress'] = ap.find('postAddress').text.strip() \
                                                    .replace('\n', '').replace('|', '/').replace('"', '')
                                                participant['protocol_type_name'] = protocol['protocol_type_name']
                                                participant['purchaseNumber'] = protocol['purchaseNumber']
                                                participant['protocolDate'] = protocol['protocolDate']
                                                participantList.append(participant)
                                        except:
                                            print('Ничего нет по участникам!')
                                            # with open('log.txt', 'at', encoding='utf-8') as file:
                                            #     file.write(f + '|Ничего нет по участникам!|' + zf + '\n')
                                            #     file.flush()

                                participantList.append(participant)
                        protocolList.append(protocol)


        except:
            copy2(self.__zip_file, bad)

        return protocolList, commissionMembersList, participantList

    def run(self):
        protocolList, commissionMembersList, participantList = self.parsezipxml()
        if protocolList is not None:
            self.__mutex.acquire()
            with open('protocols.csv', 'at', encoding='cp1251', errors='ignore') as file:
                writer = csv.DictWriter(file, ['purchaseNumber', 'protocolDate', 'signDate', 'publishDate',
                                               'docPublishDate', 'PublisherSPZ', 'PublisherName',
                                               'purchaseObjectInfo', 'protocol_type_name'],
                                        dialect='csvCommaDialect')
                writer.writerows(protocolList)

            with open('comission.csv', 'at', encoding='cp1251', errors='ignore') as file:
                writer = csv.DictWriter(file, ['purchaseNumber', 'protocolDate', 'lastName', 'firstName',
                                               'middleName', 'role', 'protocol_type_name'],
                                        dialect='csvCommaDialect')
                writer.writerows(commissionMembersList)

            with open('participants.csv', 'at', encoding='cp1251', errors='ignore') as file:
                writer = csv.DictWriter(file, ['purchaseNumber', 'protocolDate', 'appDate', 'inn',
                                               'kpp', 'organizationName', 'postAddress', 'offerPrice',
                                               'protocol_type_name'],
                                        dialect='csvCommaDialect')
                writer.writerows(participantList)
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
root_path = 'D:\\data\\common\\oos\\contracts\\ftp.zakupki.gov.ru\\fcs_regions'
# xmllist = getfiles('C:/notify_guarantee/protocols/')
xmllist = [file for file in getzipfiles(root_path) if (datetime.date.today() - modification_date(file).date()).days <= 95]
createthreadparserzip(49, xmllist)
t = time.time() - t0
print('Понадобилось времени (сек.): %fs' % t)
with open('timelog.log', 'at', encoding='cp1251', errors='ignore') as file:
    file.write('Понадобилось времени (сек.): %fs' % t + '\n')
    file.flush()
