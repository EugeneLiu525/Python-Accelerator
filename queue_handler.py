import threading
import time

class QueueHandler():
    def __init__(self):
        self.active_requests = {}
        self.condition_locks = {}
        self.global_lock = threading.Lock()
        
    def __enter__(self):
        return self
    
    def __exit__(self, *exc_info):
        pass
    
    def close(self):
        self.active_requests.clear()
        self.condition_locks.clear()
        
    def _get_condition(self, request_key):
        with self.global_lock:
            if request_key not in self.condition_locks:
                self.condition_locks[request_key] = threading.Condition()
            return self.condition_locks[request_key]

    def inqueue(self, request_key):
        condition = self._get_condition(request_key)
        with condition:
            while request_key in self.active_requests:
                condition.wait()  # 等待當前請求完成
            self.active_requests[request_key] = True

    def dequeue(self, request_key):
        condition = self._get_condition(request_key)
        with condition:
            if request_key in self.active_requests:
                del self.active_requests[request_key]
                condition.notify_all()  # 喚醒所有等待相同 request_key 的線程

    def is_inqueue(self, request_key):
        with self.global_lock:
            return request_key in self.active_requests  # 直接返回布林值

def worker(queue_handler: QueueHandler, request_key, sleep_time=1):
    print(f"Thread {threading.current_thread().name} trying to enqueue {request_key}")
    queue_handler.inqueue(request_key)
    print(f"Thread {threading.current_thread().name} has enqueued {request_key}")
    time.sleep(sleep_time)  # 模擬處理時間
    queue_handler.dequeue(request_key)
    print(f"Thread {threading.current_thread().name} has dequeued {request_key}")

def test_different_keys():
    print("Testing with different request keys:")
    num_threads = 5
    request_keys = [f"request_{i}" for i in range(num_threads)]  # 每個執行緒都有不同的 request_key

    with QueueHandler() as queue_handler:
        threads = []
        
        for key in request_keys:
            thread = threading.Thread(target=worker, args=(queue_handler, key))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()  # 等待所有執行緒完成

def test_same_key():
    print("\nTesting with the same request key:")
    num_threads = 5
    request_key = "shared_request"  # 所有執行緒使用相同的 request_key

    with QueueHandler() as queue_handler:
        threads = []
        
        for _ in range(num_threads):
            thread = threading.Thread(target=worker, args=(queue_handler, request_key, 2))  # 增加處理時間
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()  # 等待所有執行緒完成

if __name__ == "__main__":
    test_different_keys()  # 測試不同請求鍵的情況
    test_same_key()        # 測試相同請求鍵的情況
