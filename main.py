from utilities import process_record
import pymongo
import multiprocessing
import time

BATCH_SIZE = 10000  # Adjust this as per your needs
# multiprocessing.cpu_count()

def batched_cursor(cursor, batch_size):
    batch = []
    for item in cursor:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch.clear()
    if batch:
        yield batch


def database_writer(queue, batch_size=100):
    MongoClient_out = pymongo.MongoClient(host="10.12.46.33", port=27018,username="b515",password="sqwUiJGHYQTikv6z")
    collection_out = MongoClient_out['geth']['cnz_output']

    batch_data = []  # 用于保存批量数据的列表
    while True:
        data = queue.get()

        if data == "STOP":
            # 如果存在未插入的数据，插入它们
            if batch_data:
                collection_out.insert_many([{'tx_hash': item['tx_hash'],'call':item['call']} for item in batch_data])
                batch_data.clear()
            break

        batch_data.append(data)

        # 当达到批量大小时进行写入
        if len(batch_data) == batch_size:
            collection_out.insert_many([{'tx_hash': item['tx_hash'],'call':item['call']} for item in batch_data])
            batch_data.clear()  # 清空列表以准备下一批数据

def wrapper_func(args):
    return process_record(*args)

def main():
    # 远程连接服务器中mongodb，选中transaction集合
    MongoClient = pymongo.MongoClient(host="10.12.46.33", port=27018,username="b515",password="sqwUiJGHYQTikv6z")
    collection = MongoClient['geth']['transaction']

    # 查询规则
    query = {
        "tx_blocknum": {"$gt": 4000000, "$lt": 4100000},
        "tx_trace": {"$ne": ""}
    }
    cursor = collection.find(query).batch_size(1000)
    BC = batched_cursor(cursor,BATCH_SIZE)

    # 初始化消息队列
    queue = multiprocessing.Manager().Queue(1000)

    # 创建单独的数据库写入进程
    db_writer_process = multiprocessing.Process(target=database_writer, args=(queue,))
    db_writer_process.start()

    st = time.time()

    with multiprocessing.Pool(8) as pool:
        # 处理每个batch
        while True:
            try:
                # 处理一个batch
                batch = next(BC) #需要一定时间
                pool.map(wrapper_func, [(item, queue) for item in batch])
            except StopIteration:
                # 结束后关闭队列
                queue.put("STOP")
                break

    queue.put("STOP")
    db_writer_process.join()
    print(time.time()-st)

if __name__ == "__main__":
    main()