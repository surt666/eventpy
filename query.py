import boto3


def find_sag(data):
    print('Find sag')


def find_opgave(data):
    print('Find opgave')


def find_vurdering(data):
    print('Find vurdering')


def do_query(action, data):
    if action == 'find-sag':
        find_sag(data)
    elif action == 'find-opgave':
        find_opgave(data)
    elif action == 'find-vurdering':
        find_vurdering(data)


def lambda_handler(event, context):
    print(event)
    return do_query(event['action'], event['data'])
