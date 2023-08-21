
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis_om import get_redis_connection, HashModel
from starlette.requests import Request
from fastapi.background import BackgroundTasks
import requests,time

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*']
)

# Connect to redis
redis = get_redis_connection(
    host="redis-14874.c278.us-east-1-4.ec2.cloud.redislabs.com",
    port=14874,
    password="rwRkInu0Q1ZNG1NThWFbZRSkmqrSYxS4",
    decode_responses=True
)


class Product(HashModel):
    name: str
    price: float
    quantity: int

    class Meta:
        database = redis



@app.get("/products")
def all():
    return [format(pk) for pk in Product.all_pks()]

def format(pk: str):
    product = Product.get(pk)
    return {
        'id': product.pk,
        'name': product.name,
        'price': product.price,
        'quantity': product.quantity,
    }
@app.post('/products')
def create(product: Product):
    return product.save()

@app.get("/products/{pk}")
def get(pk: str):
    return  Product.get(pk)

@app.delete('/products/{pk}')
def delete(pk: str):
    return Product.delete(pk)


class Order(HashModel):
    product_id: str
    price: float
    fee: float
    total: float
    quantity: int
    status: str #pending, completed, refunded

    class Meta:
        database = redis

@app.get('/orders/{pk}')
def get(pk:str):
    return Order.get(pk)

@app.post('/orders')
async def create(request: Request, background_task: BackgroundTasks):  #we sent the id and quantity only

    try:
        body = await request.json()
        product = Product.get(body['id']).dict()
        order = Order(
            product_id = body['id'],
            price=product['price'],
            fee=0.2*product['price'],
            total=1.2 * product['price'],
            quantity = body['quantity'],
            status='pending'
        )
        order.save()
    except Exception as e:

        return "Product doesn't exist"


    #Here add the task to a background task to be resolved
    background_task.add_task(order_completed, order)


    return order

def order_completed(order: Order):
    time.sleep(5)
    order.status = 'completed'
    order.save()
    #Here we send the custom event to redis streams
    #the order of the parameters is : a key word, the data that will be managed, the id (in this case the id inside the dict so the * specific a auto id from the dict)
    redis.xadd('order_completed',order.dict(),'*')

