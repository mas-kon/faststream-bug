from fastapi import FastAPI, Request
from faststream.rabbit.fastapi import RabbitRouter
from aio_pika import RobustExchange, RobustQueue
from faststream.rabbit import (
    ExchangeType,
    RabbitExchange,
    RabbitQueue,
)
from pydantic import BaseModel

rabbit_router = RabbitRouter()

app = FastAPI(lifespan=rabbit_router.lifespan_context)


class Item(BaseModel):
    name: str
    price: float
    quantity: int


async def create_queue(name_queue: str, x_max_length: int) -> RobustQueue:
    _queue = await rabbit_router.broker.declare_queue(
        RabbitQueue(name=name_queue,
                    durable=True,
                    arguments={"x-max-length": x_max_length})
    )
    return _queue


async def create_exchange(name_exchange: str, type_exchange: ExchangeType) -> RobustExchange:
    _exchange = await rabbit_router.broker.declare_exchange(
        RabbitExchange(
            name=name_exchange,
            type=type_exchange,
        )
    )
    return _exchange


@rabbit_router.after_startup
async def setup_queues_exchanges(app: FastAPI):
    print("After STARTUP")
    my_exchange = await create_exchange(
        name_exchange="my_exchange",
        type_exchange=ExchangeType.FANOUT
    )

    my_queue = await create_queue(name_queue="my_queue",
                                  x_max_length=1000)
    await my_queue.bind(my_exchange)


@rabbit_router.post("/post-data", tags=["Data"])
async def sale_transaction(item: Item, request: Request):
    data = await request.body()
    data = data.decode("utf-8")

    await rabbit_router.broker.publish(message=data,
                                       exchange="my_exchange",
                                       queue="my_queue")
    return data


app.include_router(router=rabbit_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app="main:app",
                host="0.0.0.0",
                port=8000,
                )
