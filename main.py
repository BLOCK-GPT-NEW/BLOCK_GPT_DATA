from utilities import process_record
import pymongo
import multiprocessing
import time
import json
from multiprocessing import Semaphore


BATCH_SIZE = 1000  # Adjust this as per your needs
# multiprocessing.cpu_count()

def batched_cursor(cursor, batch_size):
    batch = []
    for item in cursor:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch=[]
    if batch:
        yield batch


import json

def database_writer(queue, batch_size=10):
    # 打开一个文件，使用"append"模式，以便如果文件已存在，则在文件末尾添加数据
    with open(r"C:\zk\BLOCK_GPT_DATA\outputs\output", "a") as file:
        cnt = 0
        batch_data = []  # 用于保存批量数据的列表

        while True:
            data = queue.get()

            if data == "STOP":
                # 如果存在未插入的数据，插入它们
                print(cnt)
                if batch_data:
                    for item in batch_data:
                        json.dump({'tx_hash': item['tx_hash'], 'call': item['call']}, file)
                        file.write('\n')  # 新的一行用于分隔每条记录
                    batch_data.clear()
                break

            cnt += 1
            batch_data.append(data)

            # 当达到批量大小时进行写入
            if len(batch_data) == batch_size:
                for item in batch_data:
                    json.dump({'tx_hash': item['tx_hash'], 'call': item['call']}, file)
                    file.write('\n')  # 新的一行用于分隔每条记录
                batch_data.clear()  # 清空列表以准备下一批数据


# def database_writer(queue, batch_size=1000):
#     MongoClient_out = pymongo.MongoClient(host="10.12.46.33", port=27018,username="b515",password="sqwUiJGHYQTikv6z")
#     collection_out = MongoClient_out['geth']['cnz_output']
#     cnt = 0
#     batch_data = []  # 用于保存批量数据的列表
#     while True:
#         data = queue.get()
#         if data == "STOP":
#             # 如果存在未插入的数据，插入它们
#             print(cnt)
#             if batch_data:
#                 collection_out.insert_many([{'tx_hash': item['tx_hash'],'call':item['call']} for item in batch_data])
#                 batch_data.clear()
#             break
#         cnt+=1
#         batch_data.append(data)

#         # 当达到批量大小时进行写入
#         if len(batch_data) == batch_size:
#             collection_out.insert_many([{'tx_hash': item['tx_hash'],'call':json.dumps(item['call'])} for item in batch_data])
#             batch_data.clear()  # 清空列表以准备下一批数据
#     MongoClient_out.close()


if __name__ == "__main__":
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
    queue = multiprocessing.Manager().Queue(100)
    
    # 创建单独的数据库写入进程
    db_writer_process = multiprocessing.Process(target=database_writer, args=(queue,))
    db_writer_process.start()

    st = time.time()
    cnt = 0
    max_tasks = 4 * 10  # 假设你希望任务队列中最多有 10 个任务
    semaphore = Semaphore(max_tasks)

    def release_semaphore(result):
        semaphore.release()

    with multiprocessing.Pool(4) as pool:
        # 处理每个batch
        while True:
            try:
                # 处理一个batch
                semaphore.acquire()
                batch = next(BC) #需要一定时间
                cnt += len(batch)
                pool.apply_async(process_record, (batch, queue,), callback=release_semaphore)
            except StopIteration:
                print(cnt)
                pool.close()  # 关闭进程池，不再接受新的任务
                pool.join()   # 等待所有任务完成
                queue.put("STOP")
                break

    db_writer_process.join()
    print(time.time()-st)