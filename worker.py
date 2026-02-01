import threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
import time
from typing import Union,Tuple,Callable
import logging

class Worker:
    def __init__(self, max_workers:int=3, verbose:bool=False):
        """
        通用 Worker 工具類，用於多線程任務處理和結果管理。
        :param max_workers: 同時運行的最大線程數
        :param verbose: 是否打印詳細日誌
        """
        self.verbose = verbose
        self.jobs = {}
        self.job_queue = Queue()
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.results = {}
        self.stop_event = threading.Event()

        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        logging.info("Worker initialized")

    def add_task(self, key:str, task_func:Union[Tuple[type,Callable],Callable], *args, **kwargs):
        """
        添加任務到隊列中。
        :param key: 任務的唯一標識符（例如股票代碼）
        :param task_func: 任務執行的函數或類的方法
        :param args: 傳入任務函數的參數
        :param kwargs: 傳入任務函數的參數
        """
        with self.lock:
            if key not in self.jobs:
                self.jobs[key] = [(task_func, args, kwargs)]
                self.job_queue.put(key)
                if self.verbose:
                    print(f"Initialized job list for {key}.")
                self.executor.submit(self._worker)
            else:
                self.jobs[key].append((task_func, args, kwargs))
                if self.verbose:
                    print(f"Added task for {key}.")

    def _worker(self):
        """ 工作線程，不斷從 job_queue 中認領任務並執行。 """
        while not self.stop_event.is_set():
            try:
                key = self.job_queue.get(timeout=10)
                if self.verbose:
                    print(f"Worker picked up {key}.")

                while key in self.jobs and self.jobs[key]:
                    with self.lock:
                        task_func, args, kwargs = self.jobs[key].pop(0)
                        if self.verbose:
                            print(f"Processing task for {key}.")
                    try:
                        if isinstance(task_func, tuple):
                            instance, method_name = task_func
                            func = getattr(instance, method_name)
                            result = func(*args, **kwargs)
                        else:
                            result = task_func(*args, **kwargs)
                        if self.verbose:
                            print(f"Task for {key} completed.")
                    except Exception as e:
                        result = e
                        print(f"Error processing task for {key}: {e}")

                    with self.condition:
                        if key not in self.results:
                            self.results[key] = []
                        self.results[key].append(result)
                        self.condition.notify_all()

                with self.lock:
                    del self.jobs[key]
                    self.job_queue.task_done()
                    if self.verbose:
                        print(f"Completed all tasks for {key}.")
            except Empty:
                break
            except Exception as e:
                print(e)

    def get_result(self, key:str, timeout:float=100.0) -> list:
        """
        等待並返回指定 key 的任務結果。
        :param key: 任務的唯一標識符
        :param timeout: 等待的最大時間，None表示無限制
        :return: 任務結果列表
        """
        with self.condition:
            start_time = time.time()
            while key not in self.results:
                if timeout is None:
                    self.condition.wait()
                else:
                    remaining_time = timeout - (time.time() - start_time)
                    if remaining_time <= 0:
                        if self.verbose:
                            print(f"Timeout reached while waiting for {key}.")
                        return None
                    self.condition.wait(timeout=remaining_time)
            return self.results.pop(key, None)

    def close(self):
        """ 停止所有工作線程並清理資源。 """
        self.stop_event.set()
        self.executor.shutdown(wait=True)
        logging.info("Worker closed")

# 範例使用
if __name__ == "__main__":
    class SampleTask:
        def __init__(self, value):
            self.value = value

        def calculate(self, x, y):
            time.sleep(2)
            return self.value + x + y

    worker = Worker(verbose=True)
    instance = SampleTask(10)

    # 添加類方法任務
    worker.add_task("task1", (instance, "calculate"), 1, 2)
    worker.add_task("task2", (instance, "calculate"), 3, 4)

    # 獲取結果
    print("Result for task1:", worker.get_result("task1", timeout=10))
    print("Result for task2:", worker.get_result("task2", timeout=10))

    # 關閉 Worker
    worker.close()
