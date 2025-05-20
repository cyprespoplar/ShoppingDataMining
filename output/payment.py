import os
from pyarrow import parquet as pq
from concurrent import futures
import json
from tqdm import tqdm
import pickle as pkl
from multiprocessing import cpu_count
from datetime import datetime
price_path = 'product_catalog.json'     
categrory_path = 'category_all.json'                
tqdm.monitor_interval = 0  # 完全禁用动态调整

def load_category(path):
    # 读取产品数据，id作为key，category和price作为value
    category = dict()
    with open(path, 'r',encoding='utf-8') as f:
        product_file = json.loads(f.read())
        for product in product_file['products']:
            category[product['id']] = {'category': product['category'],'price': product['price']}
    return category

def load_all_category(path):
    with open(path, 'r',encoding='utf-8') as f:
        product_file = json.loads(f.read())
    return product_file
category = load_category(price_path)
category_all = load_all_category(categrory_path)

# 获取商品类别信息
def collect_goods(row):
    return [ category_all[category[tag['id']]['category']] for tag in row]

def collect_all_goods(se):
    return [collect_goods(json.loads(row)['items']) for row in se]

# 获取交易支付方式
def collect_payments(se):

    return [json.loads(row)['payment_method'] for row in se]

# 获取退款情况
def collect_refunds(se):
    return [json.loads(row)['payment_status'] for row in se]
# 获取交易日期
def collect_days(se):
    return [datetime.strptime(json.loads(row)['purchase_date'],"%Y-%m-%d") for row in se]
# 获取交易物品id
def collect_id(row):
    return [tag['id'] for tag in row]

def collect_ids(se):
    return [collect_id(json.loads(row)['items']) for row in se]

def apply(file_names,operation,batchsize,info):
    """
        处理单行或是多行数据
    """

    def file_process(file_args):
        """
            内函数，定义单个处理过程
        """
        try:
            file_name,i = file_args
        except:
            raise ValueError("输入参数错误")
        assert os.path.exists(file_name),f"文件{file_name}访问错误"
        assert file_name.endswith('.parquet'),f"文件{file_name}格式错误"
        file_data = pq.ParquetFile(file_name)
        total_rows = file_data.metadata.num_rows
        results = []
        with tqdm(total=total_rows,desc=f"Processing{file_name.split('/')[-1]}",position=i) as pbar:
            for batch in file_data.iter_batches(batch_size = batchsize):
                df = batch.to_pandas()
                se = list(df['purchase_history'])
                result = operation(se)
                results.extend(result)
                pbar.update(len(df))
        return results
    workers = min(len(file_names),cpu_count())
    print("thread_num:",workers)
    func_args = [(file_name,index) for index, file_name in enumerate(file_names)]
    # exit(0)
    with futures.ThreadPoolExecutor(workers) as executor:
        results = executor.map(file_process,func_args)
    all_results = [i for item in results for i in item]
    return all_results

if __name__ == '__main__':
    sta = 'id'
    namelist = [f'./part-{i:05d}.parquet' for i in range(1)]
    if sta == 'category':
        data = apply(namelist,collect_all_goods,200000)
    elif sta == 'payment':
        data = apply(namelist,collect_payments,200000)
    elif sta == 'refund':
        data = apply(namelist,collect_refunds,200000)
    elif sta == 'day':
        data = apply(namelist,collect_days,200000)
    elif sta == 'id':
        data = apply(namelist,collect_ids,200000)
    file = open(f'good-{sta}_10G.pkl','wb')
    pkl.dump(data,file)
    # file_payment = open('good-payment_10G.pkl','wb')
    # file_refund = open('good-refund_10G.pkl','wb')
    # file_day = open('good-day_10G.pkl','wb')
    file.close()

