import boto3
import time
import calendar

opgaver = ["opgave-oprettet", "opgave-sagsbehandler-tilfoejet",
           "opgave-lukket", "opgave-startet", "opgave-stoppet"]

vurderinger = ["skoen-oprettet", "tillaeg-oprettet", "nedslag-oprettet",
               "kvm-pris-oprettet"]

sager = ["sag-oprettet", "sag-opdateret", "jp-oprettet", "jp-opdateret",
         "jn-oprettet", "jn-opdateret", "dokument-oprettet",
         "dokument-opdateret", "part-oprettet", "part-opdateret"]

dynamodb = boto3.resource('dynamodb')


def find_data_info(type):
    if type in opgaver:
        return ('opgave-events', 'opgaver', 'opgave-id')
    elif type in vurderinger:
        return ('vur-events', 'vurderinger', 'vur-ejd-id')
    elif type in sager:
        return ('sags-events', 'sager', 'sags-id')


def get_tx(type):
    table = dynamodb.Table('tx-counter')
    ret = table.update_item(Key={'type': type},
                            UpdateExpression='SET seq = seq + :incr',
                            ExpressionAttributeValues={':incr': 1},
                            ReturnValues='UPDATED_NEW')
    print(ret)
    return ret['Attributes']['seq']['N']


def create_event(type, id, tx, data):
    did = data.pop(id)
    e = {
        'type': type,
        'tx': tx,
        'oprettet': calendar.timegm(time.gmtime()),
        'payload': data
    }
    print(id)
    e[id] = did
    return e


def write_event(type, data):
    data_info = find_data_info(type)
    table = dynamodb.Table(data_info[0])
    tx = get_tx(data_info[1])
    event = create_event(type, data_info[2], tx, data)
    table.put_item(Item=event)


command2event = {
    "opret-skoen": "skoen-oprettet",
    "opret-kvm-pris": "kvm-pris-oprettet",
    "opret-tillaeg": "tillaeg-oprettet",
    "opret-nedslag": "nedslag-oprettet",
    "opret-opgave": "opgave-oprettet",
    "luk-opgave": "opgave-lukket",
    "tilfoej-sagsbehandler-opgave": "opgave-sagsbehandler-tilfoejet",
    "start-opgave": "opgave-startet",
    "stop-opgave": "opgave-stoppet"
    }


def lambda_handler(event, context):
    print(event)
    return write_event(command2event[event['action']], event['data'])
