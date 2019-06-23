from elasticsearch import Elasticsearch

def InsertData():
    es = Elasticsearch()
    es.create(
        index="my_index",
        doc_type="test_type",
        id=11,
        body={
            "name":"python",
            "addr":"sichuan"
        }
    )

    result = es.get(
        index="my_index",
        doc_type="test_type",
        id=11
    )

    print('single row inserted: \n', result)

def BulkInsertData():
    es = Elasticsearch()
    data = [
        {
            "name":"name001",
            "addr":"addr001"
        },
        {
            "name":"name002",
            "addr":"addr002"
        },
        {
            "name":"name002",
            "addr":"addr002"
        }
    ]

    for i,item in enumerate(data):
        es.create(
            index="my_index",
            doc_type="test_type",
            id=i,
            body=item
        )
    
    result = es.get(
        index="my_index",
        doc_type="test_type",
        id=0
    )

    print(' \n bulk insert finished: \n', result['_source'])

def UpdateData():
    es = Elasticsearch()
    es.update(
        index = "my_index",
        doc_type = "test_type",
        id = 11,
        body = {
            "doc":{
                "name":"update name",
                "addr":"update addr"
            }
        }
    )

    # print('\n update id 11 finished: \t', result['_source']['name'])

def DeleteDates():
    es = Elasticsearch()
    result = es.delete(
        index="my_index",
        doc_type="test_type",
        id=11
    )

def SearchByCondition():
    es = Elasticsearch()
    query1 = es.search(
        index = "my_index",
        doc_type = "test_type",
        body = {
            "query":{
                "match_all":{}
            }
        }
    )

    print('\n query all documents\n',query1)

    query2 = es.search(
        index = "my_index",
        body = {
            "query":{
                "term":{
                    "name":"name001"
                }
            }
        }
    )
    print('\n find details which name is equal to name001\n',query1)
    print('\n find documents which name is equal to name001\n',query2)