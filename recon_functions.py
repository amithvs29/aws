from boto3.dynamodb.conditions import Attr
from aws import Table


def getUnmatchedItemResponseCount(assessmentID, profile_name='dmf-prod'):
    recordTypesTable = Table(
        'pine_dmfdb_stage_sc30_record_types', profile_name=profile_name)
    filtering_exp = Attr('assessment').eq(str(assessmentID)) \
                    & Attr('itemResponseMsgId').exists() \
                    & Attr('sdqResponseMsgId').not_exists()
    items = recordTypesTable.scanTable(filtering_exp)
    return len(items)


def getUnmatchedSdqResponseCount(profile_name='dmf-prod'):
    recordTypesTable = Table(
        'pine_dmfdb_stage_sc30_record_types', profile_name=profile_name)
    filtering_exp = Attr('sdqResponseMsgId').exists() & Attr('itemResponseMsgId').not_exists()
    items = recordTypesTable.scanTable(filtering_exp)
    return len(items)


def getMatchedResponseCount(assessmentID=None, profile_name='dmf-prod'):
    recordTypesTable = Table(
        'pine_dmfdb_stage_sc30_record_types', profile_name=profile_name)
    if assessmentID:
        filtering_exp = Attr('sdqResponseMsgId').exists() \
                        & Attr('itemResponseMsgId').exists() \
                        & Attr('assessment').eq(str(assessmentID))
    else:
        filtering_exp = Attr('sdqResponseMsgId').exists() & Attr(
            'itemResponseMsgId').exists()
    items = recordTypesTable.scanTable(filtering_exp)
    return len(items)


def getASinQueue(assessmentID=None, profile_name='dmf-prod'):
    recordTypesTable = Table('pine_dmfdb_stage_sc30_record_types', profile_name=profile_name)
    filtering_exp = Attr('sdqResponseMsgId').exists() \
        & Attr('itemResponseMsgId').exists() \
        & Attr('assessment').eq(str(assessmentID)) \
        & Attr('fileName').not_exists()
    items = recordTypesTable.scanTable(filtering_exp)
    return len(items)


if __name__ == '__main__':
    print('Unmatched SDQ response count: ', getUnmatchedSdqResponseCount())
    print('Unmatched Item response count: ', getUnmatchedItemResponseCount(4))
    print('Matched response count: ', getMatchedResponseCount(4))
    print('Matched reponses in queue: ', getASinQueue(4))
