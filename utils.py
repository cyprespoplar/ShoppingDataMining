import json
price_path = 'product_catalog.json'


def load_category(path):
    # 读取产品数据，id作为key，category和price作为value
    category = dict()
    with open(path, 'r') as f:
        product_file = json.loads(f.read())
        for product in product_file['products']:
            category[product['id']] = {'category': product['category'],'price': product['price']}
    return category
a = {
'id'   ,                                     
'timestamp',                       
'user_name',                                      
'chinese_na',                                         
'email',                              
'age',                                        
'income',                                       
'gender',                                           
'country',                                          
'chinese_address' ,                           
'purchase_history' ,
'is_active',                                       
'registration_date',                                       
'credit_score'  ,      
'phone_number' } 