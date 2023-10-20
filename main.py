from utilities import process_record
import pymongo
import multiprocessing
import time
from multiprocessing import Semaphore
import sys
import json

BATCH_SIZE = 1000

# 调整以下参数 使瓶颈在远端的mongodb上
# 若提高某个参数能提升CPU占用率，说明瓶颈还在本地
#
#处理数据的进程数量
process_cnt = 1
#处理返回值的进程数量
output_handle_cnt = 1
# 写入数据库的进程数量默认为1，因为瓶颈在数据库的写入速度上

def batched_cursor(cursor, batch_size):
    batch = []
    for item in cursor:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch=[]
    if batch:
        yield batch

def process_output_handle(queue_in, queue_out, counter):
    # handle_BSONObjectTooLarge
    while True:
        item = queue_in.get()
        if item == "STOP":
            # Decrease the counter when one process has finished its tasks
            with counter.get_lock():
                counter.value -= 1
                if counter.value == 0:
                    queue_out.put("STOP")
            break
        if sys.getsizeof(json.dumps(item)) > 15 * 1024 * 1024:  # 10MB as a buffer
            print(f"Warning: Large document of size {sys.getsizeof(json.dumps(item))} bytes")
        else:
            queue_out.put({'tx_hash': item['tx_hash'],'call':item['call']})


def process_database_writer(queue, batch_size=100):
    MongoClient_out = pymongo.MongoClient(host="10.12.46.33", port=27018,username="b515",password="sqwUiJGHYQTikv6z")
    collection_out = MongoClient_out['geth']['cnz_output']
    cnt = 0
    batch_data = []  # 用于保存批量数据的列表
    while True:
        data = queue.get()
        if data == "STOP":
            # 如果存在未插入的数据，插入它们
            print(cnt)
            if batch_data:
                collection_out.insert_many(batch_data)
                batch_data.clear()
            break
        cnt+=1
        batch_data.append(data)

        # 当达到批量大小时进行写入
        if len(batch_data) == batch_size:
            collection_out.insert_many(batch_data)
            batch_data.clear()  # 清空列表以准备下一批数据
    MongoClient_out.close()


if __name__ == "__main__":
    # 远程连接服务器中mongodb，选中transaction集合
    MongoClient = pymongo.MongoClient(host="10.12.46.33", port=27018,username="b515",password="sqwUiJGHYQTikv6z")
    collection = MongoClient['geth']['transaction']

    # 查询规则
    query = {
        "tx_blocknum": {"$gt": 4000000, "$lt": 4010000},
        "tx_trace": {"$ne": ""}
    }
    cursor = collection.find(query).batch_size(500)
    BC = batched_cursor(cursor,BATCH_SIZE)

    # 初始化消息队列
    queue_process = multiprocessing.Manager().Queue(500)
    queue_db = multiprocessing.Manager().Queue(500)
    
    # 创建独立的返回数据处理进程
    counter = multiprocessing.Value('i', output_handle_cnt)
    output_handle_processes = []
    for _ in range(output_handle_cnt):
        p = multiprocessing.Process(target=process_output_handle, args=(queue_process, queue_db, counter))
        p.start()
        output_handle_processes.append(p)
    
    # 创建独立的数据库写入进程
    db_writer_process = multiprocessing.Process(target=process_database_writer, args=(queue_db,))
    db_writer_process.start()

    # 主进程
    st = time.time()
    cnt = 0
    max_tasks = process_cnt * 10
    semaphore = Semaphore(max_tasks)

    def release_semaphore(result):
        semaphore.release()

    with multiprocessing.Pool(process_cnt) as pool:
        # 处理每个batch
        while True:
            try:
                # 处理一个batch
                semaphore.acquire()
                batch = next(BC) #需要一定时间
                cnt += len(batch)
                pool.apply_async(process_record, (batch, queue_process,), callback=release_semaphore)
            except StopIteration:
                print(cnt)
                pool.close()  # 关闭进程池，不再接受新的任务
                pool.join()   # 等待所有任务完成
                # 结束所有返回数据处理进程
                for _ in range(output_handle_cnt):
                    queue_process.put("STOP")
                break

    # 等待其它进程完成
    db_writer_process.join()
    print(time.time()-st)