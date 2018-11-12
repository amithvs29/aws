from aws import S3, Table
import pandas


def read_recon_file(filename):
    data = pandas.read_csv(filename, sep='|', dtype=str, header=0)
    sdq_list = list(data['StudentEntityKey'][(data['TestID'] == 'CB-GEN-COLL-PSAT89-XSUB-SDQ_NA-10') & (data['STATUS'] == 'pending')])
    item_list = list(data['StudentEntityKey'][(data['TestID'] == 'CB-GEN-COLL-PSAT89-XSUB-NA-COMBINED-10') & (data['STATUS'] == 'pending')])
    return sdq_list, item_list


def reconDynamo(sdqList, itemList):
    t = Table('pine_dmfdb_stage_sc30_record_types', 'dmf-prod')

    sdqs = t.queryTableList(sdqList)
    sdqCount = 0
    missing_sdqs = []
    for sdq in sdqs:
        if 'sdqResponseMsgId' in sdq:
            sdqCount += 1
        else:
            missing_sdqs.append(sdq['airKey'])

    items = t.queryTableList(itemList)
    itemCount = 0
    missing_items = []
    for item in items:
        if 'itemResponseMsgId' in item:
            itemCount += 1
        else:
            missing_items.append(item['airKey'])
    return sdqCount, missing_sdqs, itemCount, missing_items


def digital_recon(filename):
    if type(filename) == list:
        sdq_list = []
        item_list = []
        for file in filename:
            a, b = read_recon_file(file)
            sdq_list.extend(a)
            item_list.extend(b)
    else:
        sdq_list, item_list = read_recon_file(filename)

    a, b, c, d = reconDynamo(sdq_list, item_list)

    s3 = S3('dmf-prod')

    missing_sdqs = set()
    if len(sdq_list):
        sdq_bucket = s3['pine-dmf-sdqresponse']
        all_sdq_list = [str(i.key)[0:5] for i in sdq_bucket.bucket.objects.all()]
        missing_sdqs = set(sdq_list) - set(all_sdq_list)
    print("Number of SDQ responses received: {}".format(len(sdq_list)))
    print("Number of SDQ responses missing: {}".format(len(missing_sdqs)))
    print(missing_sdqs)
    print("Number of SDQ responses loaded into DynamoDB: {}".format(a))
    print("Number of SDQs not loaded: {}". format(len(b)))
    print(b)
    print()

    missing_items = set()
    if len(item_list):
        item_bucket = s3['pine-dmf-itemresponse']
        all_item_list = [str(i.key)[0:5] for i in item_bucket.bucket.objects.all()]
        missing_items = set(item_list) - set(all_item_list)
    print("Number of item responses received: {}".format(len(item_list)))
    print("Number of item responses missing: {}".format(len(missing_items)))
    print(missing_items)
    print("Number of Item responses loaded into DynamoDB: {}".format(c))
    print("Number of Items not loaded: {}". format(len(d)))
    print(d)


def checkDupes(filename):
    if type(filename) == list:
        sdq_list = []
        item_list = []
        for file in filename:
            a, b = read_recon_file(file)
            sdq_list.extend(a)
            item_list.extend(b)
    from collections import Counter
    a, b = Counter(sdq_list), Counter(item_list)
    x, y = [], []
    for k, v in a.items():
        if v > 1:
            x.append(k)
    for k, v in b.items():
        if v > 1:
            y.append(k)
    print(x)
    print(y)


if __name__ == "__main__":
    files = [r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\Reconciliation_20181006040000_Fall2018.txt',
             r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\Reconciliation_20181008040000_Fall2018.txt',
             r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\Reconciliation_20181009040000_Fall2018.txt',
             r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\Reconciliation_20181010040000_Fall2018.txt',
             r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\Reconciliation_20181011040000_Fall2018.txt',
             r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\Reconciliation_20181012040000_Fall2018.txt',
             r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\Reconciliation_20181013040000_Fall2018.txt',
             r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\Reconciliation_20181014040000_Fall2018.txt',
             r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\Reconciliation_20181015040000_Fall2018.txt',
             r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\Reconciliation_20181016040000_Fall2018.txt',
             r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\Reconciliation_20181017040000_Fall2018.txt',
             r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\Reconciliation_20181018040000_Fall2018.txt'
             ]
    # digital_recon(r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\Reconciliation_20181023040000_Fall2018.txt')
    sdqList, itemList = read_recon_file(r'\\rodska10\sat_share\Datafeed Group Docs\Datafeed Calendar\2018 Scoring\Digital fall 2018\Recon files\IncompleteRecon\PendingCombo_Reconciliation_20181108040000_Fall2018.txt')
    print(len(sdqList), len(itemList))
    # a,b,c,d = reconDynamo(sdqList, itemList)
    # print(a, len(b), c, len(d))
