import os
from pyarrow import parquet as pq
from concurrent import futures
import json
from tqdm import tqdm
import pickle as pkl
from multiprocessing import cpu_count
price_path = 'product_catalog.json'                     
tqdm.monitor_interval = 0  # 完全禁用动态调整

def load_category(path):
    # 读取产品数据，id作为key，category和price作为value
    category = dict()
    with open(path, 'r',encoding='utf-8') as f:
        product_file = json.loads(f.read())
        for product in product_file['products']:
            category[product['id']] = {'category': product['category'],'price': product['price']}
    return category
category = load_category(price_path)


def collect_goods(row):
    return [category[tag['id']]['category'] for tag in row]

def collect_all_goods(se):

    return [collect_goods(json.loads(row)['items']) for row in se]

def apply(file_names,operation,batchsize):
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
   
    namelist = [f'./part-{i:05d}.parquet' for i in range(1)]
    file = open('good-good_10G.pkl','wb')
    data = apply(namelist,collect_all_goods,200000)
    pkl.dump(data,file)
    file.close()
