import asyncio
import time 

sleep_time = 0.1

async def A():
    b = asyncio.create_task( B() )
    c = asyncio.create_task( C() )    
    await b 
    await c
    # do something

    time.sleep(sleep_time)

async def B():
    time.sleep(sleep_time)

async def C():
    D()
    D()

def D():
    time.sleep(sleep_time)


def A():
    B()
    C()
    # do something

if __name__ == '__main__':
    asyncio.run(A())
