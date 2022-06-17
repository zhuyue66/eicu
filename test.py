import threading
import pandas as pd

class MyThread(threading.Thread):
    def __init__(self, thread_name):
        # 注意：一定要显式的调用父类的初始化函数。
        super(MyThread, self).__init__(name=thread_name)

    def run(self):
        print("%s正在运行中......" % self.name)

if __name__ == '__main__':

    df = pd.read_csv('./file/test.csv', sep=',')
    print(df.shape)
    df_list = []
    # 切片
    for i in range(10):
        df_list.append(df[i*10:i*10+10])

    print(len(df_list))
    for a in df_list:
        print(a)

    for i in range(10):
        MyThread("thread-" + str(i)).start()


class A:

    def __int__(self):
        self.a = 'a'

    def getA(self):
        print(self.a)

if __name__ == '__main__':
    A().getA()